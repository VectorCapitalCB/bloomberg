package cl.vc.arb.apps.fh.ss;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.ingest.BbgTick;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import com.google.protobuf.util.Timestamps;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static cl.vc.arb.apps.fh.utils.Helper.LOGON_FIX;

/**
 * Actor de topic: una instancia por instrumento suscrito. Reemplaza al antiguo InversorFixToProto.
 *
 * <p>Convierte los ticks top-of-book que entrega Bloomberg ({@link BbgTick}) en los mensajes proto
 * del protocolo ({@code Statistic} / {@code DataBook} / {@code Trade}), los acumula en un
 * {@link BookSnapshot} y los publica al {@code MessageEventBus} para el fan-out (TopicDistributor).
 * Mantiene tambien el modo simulador (sin proveedor) heredado del ORB-BCS.</p>
 */
@Slf4j
public class InversorBloombergToProto extends AbstractActor {

    private final MessageEventBus eventBus;

    private String id;

    private BookSnapshot bookSnapshot;

    private final MarketDataMessage.Subscribe subscribe;

    private int maxTradesPerSymbol = 2000;

    private String bbgSecurity;

    private boolean firstTickLogged = false;

    /** Guards against starting the simulator scheduler more than once. */
    private final AtomicBoolean simulatorStarted = new AtomicBoolean(false);

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private final ScheduledExecutorService schedulerNew = Executors.newSingleThreadScheduledExecutor();

    public static Props props(MarketDataMessage.Subscribe subscribe, MessageEventBus eventBus) {
        return Props.create(InversorBloombergToProto.class, subscribe, eventBus);
    }

    public InversorBloombergToProto(MarketDataMessage.Subscribe subscribe, MessageEventBus eventBus) {
        try {
            this.subscribe = subscribe;
            this.eventBus = eventBus;
            this.maxTradesPerSymbol = resolveMaxTradesPerSymbol();
            initializeState();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    private void initializeState() {
        id = TopicGenerator.getTopicMKD(subscribe);
        bookSnapshot = new BookSnapshot(id, subscribe);
        MainApp.bookHasmap.put(id, bookSnapshot);
    }

    @Override
    public void preStart() {
        createSubscriptor();
    }

    @Override
    public void postStop() {
        scheduler.shutdownNow();
        schedulerNew.shutdownNow();
        try {
            BloombergGateway gw = MainApp.getBloombergGateway();
            if (gw != null) {
                gw.unsubscribe(id, getSelf());
            }
        } catch (Exception e) {
            log.warn("bloomberg unsubscribe cleanup failed topic={} reason={}", id, e.getMessage());
        }
        eventBus.unsubscribe(getSelf());
        MainApp.bookHasmap.remove(id);
    }

    private void createSubscriptor() {
        try {
            boolean isSimulator = "yes".equalsIgnoreCase(
                    MainApp.getProperties().getProperty("simulador", "no"));

            log.info("subscriber init topic={} clientSubId={} simulador={}", id, subscribe.getId(), isSimulator);

            if (isSimulator) {
                startSimulator();
                return;
            }

            BloombergGateway gw = MainApp.getBloombergGateway();
            if (gw == null) {
                log.warn("bloomberg gateway no disponible topic={} (¿build sin -Pbloomberg o sesion no iniciada?)", id);
                return;
            }

            this.bbgSecurity = resolveBbgSecurity();
            List<String> fields = resolveFields();
            log.info("bloomberg subscribe topic={} security='{}' fields={}", id, bbgSecurity, fields);
            gw.subscribe(id, bbgSecurity, fields, getSelf());

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Mapea el symbol del cliente al ticker Bloomberg. Configurable con bloomberg.security.suffix. */
    private String resolveBbgSecurity() {
        String sym = subscribe.getSymbol();
        if (sym == null) return "";
        if (sym.contains(" ")) {
            return sym; // ya parece un ticker Bloomberg completo ("IBM US Equity")
        }
        String suffix = MainApp.getProperties().getProperty("bloomberg.security.suffix", "").trim();
        return suffix.isEmpty() ? sym : sym + " " + suffix;
    }

    /** Campos a suscribir en //blp/mktdata (top-of-book). Configurable con bloomberg.fields. */
    private List<String> resolveFields() {
        String configured = MainApp.getProperties().getProperty("bloomberg.fields",
                "BID,ASK,BID_SIZE,ASK_SIZE,LAST_PRICE,SIZE_LAST_TRADE,VOLUME,OPEN,HIGH,LOW,PREV_SES_LAST_PRICE,MKTDATA_EVENT_TYPE");
        return Arrays.stream(configured.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(BbgTick.class, this::onBbgTick)
                .match(SendSnapshot.class, this::onSendSnapshot)
                .build();
    }

    @Data
    public static class SendSnapshot {
        private final MarketDataMessage.Subscribe subscribe;
        private final ActorRef actorRef;
    }

    private void onSendSnapshot(SendSnapshot msg) {
        try {
            log.info("send cached snapshot topic={} targetActor={} clientSubId={} snapshotReady={}",
                    id, msg.actorRef, msg.subscribe.getId(), bookSnapshot != null && bookSnapshot.getReceivedSnapshot());
            msg.actorRef.tell(bookSnapshot, ActorRef.noSender());
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    // ------------------------------------------------------------------ //
    //  Bloomberg tick -> proto (top-of-book)                              //
    // ------------------------------------------------------------------ //

    private void onBbgTick(BbgTick tick) {
        try {
            MainApp.getBloombergTicks().increment();
            MainApp.markData();
            if (!firstTickLogged) {
                firstTickLogged = true;
                log.info("DATA OK topic={} security='{}' (primer tick recibido)", id,
                        bbgSecurity != null ? bbgSecurity : subscribe.getSymbol());
            }

            Map<String, Double> n = tick.nums;
            MarketDataMessage.Statistic.Builder stat = bookSnapshot.getStatistic();
            MarketDataMessage.Ohlcv.Builder ohlcv = bookSnapshot.getOhlcv();

            if (n.containsKey("BID")) {
                double bid = n.get("BID");
                stat.setBidPx(bid);
                setTopBook(MarketDataMessage.TypeBook.BID, bid, n.getOrDefault("BID_SIZE", 0d));
            }
            if (n.containsKey("BID_SIZE")) stat.setBidQty(n.get("BID_SIZE"));

            if (n.containsKey("ASK")) {
                double ask = n.get("ASK");
                stat.setAskPx(ask);
                setTopBook(MarketDataMessage.TypeBook.ASK, ask, n.getOrDefault("ASK_SIZE", 0d));
            }
            if (n.containsKey("ASK_SIZE")) stat.setAskQty(n.get("ASK_SIZE"));

            if (n.containsKey("VOLUME")) {
                ohlcv.setVolume(n.get("VOLUME"));
                stat.setVolume(n.get("VOLUME"));
            }
            if (n.containsKey("OPEN")) {
                ohlcv.setOpen(n.get("OPEN"));
                stat.setOpen(n.get("OPEN"));
            }
            if (n.containsKey("HIGH")) {
                ohlcv.setHigh(n.get("HIGH"));
                stat.setHigh(n.get("HIGH"));
            }
            if (n.containsKey("LOW")) {
                ohlcv.setLow(n.get("LOW"));
                stat.setLow(n.get("LOW"));
            }
            if (n.containsKey("PREV_SES_LAST_PRICE")) stat.setPreviusClose(n.get("PREV_SES_LAST_PRICE"));
            if (n.containsKey("PX_YEST_CLOSE")) stat.setPreviusClose(n.get("PX_YEST_CLOSE"));

            // Trade print: LAST_PRICE (+ SIZE_LAST_TRADE), o evento explicito TRADE.
            Double last = n.get("LAST_PRICE");
            boolean isTrade = (tick.eventType != null && tick.eventType.toUpperCase().contains("TRADE"))
                    || n.containsKey("SIZE_LAST_TRADE");
            if (last != null && last > 0d) {
                stat.setLast(last);
                if (ohlcv.getHigh() == 0d || last > ohlcv.getHigh()) ohlcv.setHigh(last);
                if (ohlcv.getLow() == 0d || last < ohlcv.getLow()) ohlcv.setLow(last);

                if (isTrade) {
                    double qty = n.getOrDefault("SIZE_LAST_TRADE", 0d);
                    MarketDataMessage.Trade.Builder trade = MarketDataMessage.Trade.newBuilder()
                            .setPrice(last)
                            .setQty(qty)
                            .setAmount(last * qty)
                            .setIdGenerico(IDGenerator.getID())
                            .setId(bookSnapshot.getId())
                            .setSymbol(bookSnapshot.getSymbol())
                            .setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData())
                            .setSettlType(bookSnapshot.getSettlType())
                            .setT(Timestamps.fromMillis(System.currentTimeMillis()));
                    addTradeWithCap(trade.build());
                    eventBus.publish(new Envelope(bookSnapshot.getId(), trade));
                }
            }

            stat.setOhlcv(ohlcv);
            bookSnapshot.setReceivedSnapshot(true);
            eventBus.publish(new Envelope(bookSnapshot.getId(), bookSnapshot));

        } catch (Exception ex) {
            log.error("bloomberg tick error topic={}", id, ex);
        }
    }

    /** Top-of-the-book: una sola postura por lado (mejor BID / mejor ASK). */
    private void setTopBook(MarketDataMessage.TypeBook side, double price, double size) {
        MarketDataMessage.DataBook db = MarketDataMessage.DataBook.newBuilder()
                .setSymbol(bookSnapshot.getSymbol())
                .setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData())
                .setTypeBook(side)
                .setPrice(price)
                .setSize(size)
                .build();
        List<MarketDataMessage.DataBook> list =
                (side == MarketDataMessage.TypeBook.BID) ? bookSnapshot.getBid() : bookSnapshot.getAsk();
        list.clear();
        list.add(db);
    }

    private int resolveMaxTradesPerSymbol() {
        String configured = MainApp.getProperties().getProperty("marketdata.max.trades.per.symbol", "2000");
        try {
            int parsed = Integer.parseInt(configured);
            return parsed > 0 ? parsed : 2000;
        } catch (NumberFormatException ex) {
            log.warn("invalid marketdata.max.trades.per.symbol='{}', using default=2000", configured);
            return 2000;
        }
    }

    private void addTradeWithCap(MarketDataMessage.Trade trade) {
        if (maxTradesPerSymbol > 0 && bookSnapshot.getTrades().size() >= maxTradesPerSymbol) {
            bookSnapshot.getTrades().removeFirst();
        }
        bookSnapshot.getTrades().add(trade);
    }

    // ------------------------------------------------------------------ //
    //  Simulator mode (sin proveedor)                                      //
    // ------------------------------------------------------------------ //

    private void startSimulator() {
        if (!simulatorStarted.compareAndSet(false, true)) {
            log.info("simulator already running topic={}", id);
            return;
        }

        int refreshMs = Integer.parseInt(
                MainApp.getProperties().getProperty("simulador.refresh.ms", "1000"));

        SimulatorPriceRef ref = MainApp.getSimulatorPrices().get(bookSnapshot.getSymbol());
        if (ref == null) {
            log.warn("simulator no reference price for symbol={}, using default ref=100", bookSnapshot.getSymbol());
            ref = new SimulatorPriceRef(bookSnapshot.getSymbol(), 100.0, 100.0, 0.0, 1000, 1000);
        }

        final SimulatorPriceRef finalRef = ref;

        log.info("simulator starting topic={} symbol={} refPx={} refreshMs={}",
                id, bookSnapshot.getSymbol(), finalRef.getLast(), refreshMs);

        SimulatorEngine.populate(bookSnapshot, finalRef);
        eventBus.publish(new Envelope(bookSnapshot.getId(), bookSnapshot));

        schedulerNew.scheduleAtFixedRate(() -> {
            try {
                String msg = "Simulador " + MainApp.securityExchange.name() + " - " + bookSnapshot.getSymbol() + " activo";
                MarketDataMessage.News news = MarketDataMessage.News.newBuilder()
                        .setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData())
                        .setLineoftext(msg).setTexto(msg).build();
                MainApp.getNews().add(news);
                MainApp.getEventBus().publish(new Envelope(LOGON_FIX, news));
            } catch (Exception e) {
                log.error("simulator news error topic={}", id, e);
            }
        }, 5, 5, TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(() -> {
            try {
                SimulatorEngine.populate(bookSnapshot, finalRef);
                MainApp.markData();

                if (!bookSnapshot.getTrades().isEmpty()) {
                    MarketDataMessage.Trade last = bookSnapshot.getTrades()
                            .get(bookSnapshot.getTrades().size() - 1);
                    MarketDataMessage.Trade.Builder tradeBuilder = last.toBuilder()
                            .setId(bookSnapshot.getId());
                    eventBus.publish(new Envelope(bookSnapshot.getId(), tradeBuilder));
                }

                eventBus.publish(new Envelope(bookSnapshot.getId(), bookSnapshot));

            } catch (Exception e) {
                log.error("simulator update error topic={}", id, e);
            }
        }, refreshMs, refreshMs, TimeUnit.MILLISECONDS);
    }
}
