package cl.vc.arb.apps.fh;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import cl.vc.arb.apps.fh.kafka.SendKafkaAndMongo;
import cl.vc.arb.apps.fh.ss.GlobalTradeSimulator;
import cl.vc.arb.apps.fh.ss.SimulatorPriceLoader;
import cl.vc.arb.apps.fh.ss.SimulatorPriceRef;
import cl.vc.arb.apps.fh.ss.SellSideManager;
import cl.vc.arb.apps.fh.tcp.ClientManager;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.arb.apps.fh.utils.Helper;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufServer;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MainApp {

    public static MarketDataMessage.SecurityExchangeMarketData securityExchange;

    public static MarketDataMessage.SecurityList.Builder securityList = MarketDataMessage.SecurityList.newBuilder();

    public static Map<String, BookSnapshot> bookHasmap = new ConcurrentHashMap<>();

    public static Map<String, MarketDataMessage.Security> securityListMaps = new ConcurrentHashMap<>();

    public static Map<String, String> symbolSuscribeMaps = new ConcurrentHashMap<>();

    public static Map<String, ActorRef> sessionTrackerMaps = new ConcurrentHashMap<>();

    @Getter
    private static final Map<String, String> channelSessionMaps = new ConcurrentHashMap<>();

    public static Map<String, ActorRef> inversorFixToProtoMaps = new ConcurrentHashMap<>();

    @Getter
    private static final Map<String, ActorRef> inversorFixLookup = new ConcurrentHashMap<>();

    @Getter
    private static ActorRef sellSideManager;


    @Getter
    private static final Map<String, Map<String, ActorRef>> actorsSubscribe = new ConcurrentHashMap<>();

    @Getter
    private static final Map<String, ActorRef> topicDistributorMaps = new ConcurrentHashMap<>();

    @Getter
    private static final java.util.concurrent.atomic.LongAdder outboundWrites = new java.util.concurrent.atomic.LongAdder();

    @Getter
    private static final java.util.concurrent.atomic.LongAdder outboundSkipped = new java.util.concurrent.atomic.LongAdder();

    @Getter
    private static final java.util.concurrent.atomic.LongAdder outboundEvicted = new java.util.concurrent.atomic.LongAdder();

    @Getter
    private static final java.util.concurrent.atomic.LongAdder activeTopicSubscribers = new java.util.concurrent.atomic.LongAdder();

    /** Contador de ticks de Bloomberg procesados (para saber si esta llegando data). */
    @Getter
    private static final java.util.concurrent.atomic.LongAdder bloombergTicks = new java.util.concurrent.atomic.LongAdder();

    /** Epoch ms del ultimo dato recibido (tick real o update de simulador). 0 = sin data aun. */
    private static final java.util.concurrent.atomic.AtomicLong lastDataMs = new java.util.concurrent.atomic.AtomicLong(0);

    /** Marca que acaba de llegar/actualizarse data (lo llaman el actor de ticks y el simulador). */
    public static void markData() {
        lastDataMs.set(System.currentTimeMillis());
    }

    public static long getLastDataMs() {
        return lastDataMs.get();
    }

    /** Estado del monitor de latencia: true si ya avisamos que la data esta estancada. */
    private static volatile boolean staleNotified = false;

    /** Bloomberg ingest gateway (BLPAPI). Null en simulador o si el build no incluye bbg/ (-Pbloomberg). */
    @Getter
    @Setter
    private static cl.vc.arb.apps.fh.ingest.BloombergGateway bloombergGateway;

    @Getter
    private static final long startTimeMs = System.currentTimeMillis();

    private static cl.vc.arb.apps.fh.admin.AdminServer adminServer;

    @Getter
    @Setter
    private static Properties properties;

    @Getter
    private static ActorRef actorKafka;

    @Getter
    private static ActorRef clientManager;

    @Getter
    private static final MessageEventBus eventBus = new MessageEventBus();

    private static NettyProtobufServer nettyProtobufServer;

    @Getter
    private static ZoneId zoneId;

    @Getter
    private static ActorSystem system;

    @Getter
    private static final Set<MarketDataMessage.News> news = ConcurrentHashMap.newKeySet();

    /** Reference prices loaded from simulator-prices.json when simulador=yes. */
    @Getter
    private static Map<String, SimulatorPriceRef> simulatorPrices = new ConcurrentHashMap<>();


    private static final int CAPACITY = 200_000;
    private static final BlockingQueue<Runnable> Q = new ArrayBlockingQueue<>(CAPACITY);
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);


    private static final Thread WORKER = new Thread(() -> {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Q.take().run();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                log.error("EventBusSafe worker error", t);
            }
        }
    }, "eventbus-safe-worker");

    private static final ScheduledExecutorService DIAGNOSTICS =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "orb-diagnostics");
                t.setDaemon(true);
                return t;
            });

    private static final AtomicBoolean DIAGNOSTICS_STARTED = new AtomicBoolean(false);

    private static void enqueue(Runnable r) {
        if (!STARTED.get()) start();

        boolean ok = Q.offer(r);
        if (!ok) {
            // Importante: NO bloquear aquí para que no se congele el app.
            log.warn("EventBusSafe queue FULL ({}). Dropping job.", CAPACITY);
        }
    }

    public static void start() {
        if (STARTED.compareAndSet(false, true)) {
            WORKER.setDaemon(true);
            WORKER.start();
        }
    }

    public static void main(String[] args) {

        try {
            init(args);
        } catch (Exception ex) {
            log.error("Cannot start Application...", ex);
        }

    }

    public static void init(String[] args) {

        try {

            try (FileInputStream fis = new FileInputStream(args[0])) {
                properties = new Properties();
                properties.load(fis);
            }

            zoneId = ZoneId.of(properties.getProperty("zoneId"));
            TimeZone.setDefault(TimeZone.getTimeZone(zoneId));

            system = ActorSystem.create();

            // Centro de notificaciones (persistencia Redis + toasts en el front).
            cl.vc.arb.apps.fh.notif.NotificationCenter.get().init(properties);

            securityExchange = resolveSecurityExchange(properties.getProperty("securityExchange"));

            sellSideManager = system.actorOf(SellSideManager.props(properties, eventBus));
            clientManager = system.actorOf(ClientManager.props());

            actorKafka = system.actorOf(SendKafkaAndMongo.props(properties));

            log.info("App... {}", securityExchange.name());

            boolean isSimulator = "yes".equalsIgnoreCase(properties.getProperty("simulador", "no"));
            if (isSimulator) {
                String pricesFile = properties.getProperty("simulador.prices.file");
                simulatorPrices = SimulatorPriceLoader.load(pricesFile);
                int refreshMs = Integer.parseInt(properties.getProperty("simulador.refresh.ms", "1000"));
                system.actorOf(GlobalTradeSimulator.props(simulatorPrices, refreshMs, securityExchange));
                log.info("simulator mode enabled prices={} refreshMs={}", simulatorPrices.size(), refreshMs);
            }

            boolean nettyPayloadLogging = Boolean.parseBoolean(
                    properties.getProperty("netty.payload.logging", "false"));

            nettyProtobufServer =
                    new NettyProtobufServer(properties.getProperty("server.host"),
                            clientManager, properties.getProperty("path.logs"),
                            securityExchange.name(), nettyPayloadLogging);

            new Thread(nettyProtobufServer).start();

            primeConfiguredSymbols();

            if (Boolean.parseBoolean(properties.getProperty("admin.enabled", "false"))) {
                int adminPort = Integer.parseInt(properties.getProperty("admin.port", "8090"));
                adminServer = new cl.vc.arb.apps.fh.admin.AdminServer(adminPort);
                adminServer.start();
            }

            if (Boolean.parseBoolean(properties.getProperty("gui.enabled", "true"))
                    && !java.awt.GraphicsEnvironment.isHeadless()) {
                try {
                    new cl.vc.arb.apps.fh.gui.AdminWindow().show();
                } catch (Throwable t) {
                    log.warn("no se pudo abrir la ventana de administracion", t);
                }
            }

            start();
            startDiagnostics();
            autoSubscribeConfiguredSymbols();

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private static void startDiagnostics() {
        boolean enabled = Boolean.parseBoolean(properties.getProperty("diagnostics.enabled", "true"));
        if (!enabled || !DIAGNOSTICS_STARTED.compareAndSet(false, true)) {
            return;
        }

        long intervalMs = Long.parseLong(properties.getProperty("diagnostics.interval.ms", "30000"));
        DIAGNOSTICS.scheduleAtFixedRate(() -> {
            try {
                long readySnapshots = bookHasmap.values().stream()
                        .filter(BookSnapshot::getReceivedSnapshot)
                        .count();

                log.info("health exchange={} eventBusQueue={} sessions={} topicDistributors={} topicSubscribers={} fixActors={} books={} readyBooks={} fixSubscriptions={} channelSessions={} outboundWrites={} outboundSkipped={} outboundEvicted={} kafkaActor={}",
                        securityExchange,
                        queued(),
                        sessionTrackerMaps.size(),
                        topicDistributorMaps.size(),
                        activeTopicSubscribers.sum(),
                        inversorFixToProtoMaps.size(),
                        bookHasmap.size(),
                        readySnapshots,
                        symbolSuscribeMaps.size(),
                        channelSessionMaps.size(),
                        outboundWrites.sum(),
                        outboundSkipped.sum(),
                        outboundEvicted.sum(),
                        actorKafka != null);

                log.info("DATA exchange={} ticksRecibidos={} suscripcionesActivas={} booksListos={} bloombergConectado={}",
                        securityExchange, bloombergTicks.sum(), bookHasmap.size(), readySnapshots, bloombergGateway != null);
            } catch (Exception e) {
                log.warn("health diagnostics failed", e);
            }
        }, intervalMs, intervalMs, TimeUnit.MILLISECONDS);

        log.info("health diagnostics enabled intervalMs={}", intervalMs);

        startLatencyMonitor();
    }

    /**
     * Monitor de latencia/staleness con Bloomberg: si estamos conectados, hay suscripciones y no
     * llega data hace mas de {@code bloomberg.stale.threshold.ms}, publica una alerta de LATENCIA;
     * cuando la data se normaliza, avisa la recuperacion. Solo aplica con ingesta en vivo.
     */
    private static void startLatencyMonitor() {
        boolean enabled = Boolean.parseBoolean(properties.getProperty("bloomberg.latency.monitor.enabled", "true"));
        if (!enabled) {
            return;
        }
        long threshold = Long.parseLong(properties.getProperty("bloomberg.stale.threshold.ms", "8000"));
        long check = Long.parseLong(properties.getProperty("bloomberg.latency.check.ms", "3000"));

        DIAGNOSTICS.scheduleAtFixedRate(() -> {
            try {
                boolean live = bloombergGateway != null && bloombergGateway.isConnected();
                if (!live || bookHasmap.isEmpty()) {
                    return;
                }
                long last = lastDataMs.get();
                if (last == 0) {
                    return; // todavia no llega el primer dato
                }
                long gap = System.currentTimeMillis() - last;
                if (gap > threshold && !staleNotified) {
                    staleNotified = true;
                    cl.vc.arb.apps.fh.notif.NotificationCenter.get().publish(
                            cl.vc.arb.apps.fh.notif.NotificationType.LATENCIA, "Latencia Bloomberg",
                            "sin data hace " + (gap / 1000) + "s (¿lag o feed caido?)");
                } else if (gap <= threshold && staleNotified) {
                    staleNotified = false;
                    cl.vc.arb.apps.fh.notif.NotificationCenter.get().publish(
                            cl.vc.arb.apps.fh.notif.NotificationType.CONEXION, "Latencia Bloomberg",
                            "data normalizada");
                }
            } catch (Exception e) {
                log.debug("latency monitor error: {}", e.getMessage());
            }
        }, check, check, TimeUnit.MILLISECONDS);

        log.info("latency monitor enabled thresholdMs={} checkMs={}", threshold, check);
    }

    public static void suscribe(String id, ActorRef actorRef) {
        enqueue(() -> {
            try {

            } catch (Exception ex) {
                log.error("subscribe error id={}", id, ex);
            }
        });
    }

    public static void unsuscribe(String id, ActorRef actorRef) {
        enqueue(() -> {
            try {
                MainApp.getEventBus().unsubscribe(actorRef, id);
            } catch (Exception ex) {
                log.error("unsubscribe error id={}", id, ex);
            }
        });
    }

    public static int queued() {
        return Q.size();
    }

    /**
     * Precarga los papeles bootstrap en memoria para que aparezcan en la grilla desde el arranque,
     * incluso si Bloomberg/simulador aun no entrega ticks.
     */
    private static void primeConfiguredSymbols() {
        for (MarketDataMessage.Subscribe sub : configuredBootstrapSubscriptions()) {
            String topic = cl.vc.module.protocolbuff.generator.TopicGenerator.getTopicMKD(sub);
            bookHasmap.putIfAbsent(topic, new BookSnapshot(topic, sub));
            log.info("bootstrap primed symbol='{}' topic='{}'", sub.getSymbol(), topic);
        }
    }

    /**
     * Suscribe papeles bootstrap para que queden visibles en desktop/web y listos para clientes
     * externos apenas la app arranca.
     */
    private static void autoSubscribeConfiguredSymbols() {
        for (MarketDataMessage.Subscribe sub : configuredBootstrapSubscriptions()) {
            sellSideManager.tell(new SellSideManager.Subscribe(sub, ActorRef.noSender()), ActorRef.noSender());
            log.info("bootstrap auto-subscribe symbol='{}'", sub.getSymbol());
        }
    }

    private static List<MarketDataMessage.Subscribe> configuredBootstrapSubscriptions() {
        String configured = properties.getProperty("bootstrap.subscribe.symbols", "").trim();
        if (configured.isEmpty()) {
            return Collections.emptyList();
        }

        Set<String> symbols = new LinkedHashSet<>();
        for (String token : configured.split("[,;\\r\\n]+")) {
            String symbol = token == null ? "" : token.trim();
            if (!symbol.isEmpty()) {
                symbols.add(symbol);
            }
        }
        if (symbols.isEmpty()) {
            return Collections.emptyList();
        }

        List<MarketDataMessage.Subscribe> subscriptions = new ArrayList<>();
        int index = 1;
        for (String symbol : symbols) {
            subscriptions.add(MarketDataMessage.Subscribe.newBuilder()
                    .setSymbol(symbol)
                    .setId("bootstrap-" + index++)
                    .setSecurityExchange(securityExchange)
                    .setSettlType(RoutingMessage.SettlType.REGULAR)
                    .setTrade(true)
                    .setStatistic(true)
                    .setBook(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .build());
        }
        return subscriptions;
    }

    /**
     * Compatibilidad con configs antiguas/nuevas: si el proto compartido todavia no trae
     * BLOOMBERG_MKD, degradamos a NONE_MKD para no impedir el arranque.
     */
    private static MarketDataMessage.SecurityExchangeMarketData resolveSecurityExchange(String configured) {
        String value = configured == null ? "" : configured.trim();
        try {
            return MarketDataMessage.SecurityExchangeMarketData.valueOf(value);
        } catch (IllegalArgumentException ex) {
            if ("BLOOMBERG_MKD".equalsIgnoreCase(value)) {
                log.warn("securityExchange={} no existe en principal-module; usando NONE_MKD temporalmente", value);
                return MarketDataMessage.SecurityExchangeMarketData.NONE_MKD;
            }
            throw ex;
        }
    }

}
