package cl.vc.arb.apps.fh.remote;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.notif.NotificationCenter;
import cl.vc.arb.apps.fh.notif.NotificationType;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import com.google.protobuf.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class RemoteOrbClient extends AbstractActor {

    private final Properties properties;
    private final ConcurrentMap<String, MarketDataMessage.Subscribe> byId = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, String> topicById = new ConcurrentHashMap<>();
    private NettyProtobufClient client;

    public static Props props(Properties properties) {
        return Props.create(RemoteOrbClient.class, properties);
    }

    public RemoteOrbClient(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void preStart() throws Exception {
        String host = properties.getProperty("remote.client.host", "172.16.0.8:8050").trim();
        String id = properties.getProperty("remote.client.id", "orb-bloomberg-desktop").trim();
        client = new NettyProtobufClient(
                host,
                getSelf(),
                properties.getProperty("path.logs", "logs"),
                MainApp.securityExchange.name(),
                NotificationMessage.Component.ORB_BCS,
                false,
                id);
        client.start();
        MainApp.setRemoteClientHost(host);
        log.info("remote ORB client connecting host={} id={}", host, id);
    }

    @Override
    public void postStop() {
        if (client != null) {
            client.stopClient();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onTransport)
                .build();
    }

    private void onTransport(TransportingObjects transport) {
        Message msg = transport.getMessage();
        try {
            if (msg instanceof SessionsMessage.Connect) {
                onConnected();
            } else if (msg instanceof SessionsMessage.Disconnect) {
                MainApp.setRemoteClientConnected(false);
                NotificationCenter.get().publish(NotificationType.DESCONEXION, "ORB remoto", "desconectado");
            } else if (msg instanceof MarketDataMessage.Snapshot snapshot) {
                onSnapshot(snapshot);
            } else if (msg instanceof MarketDataMessage.Statistic statistic) {
                onStatistic(statistic);
            } else if (msg instanceof MarketDataMessage.IncrementalBook book) {
                onIncrementalBook(book);
            } else if (msg instanceof MarketDataMessage.Trade trade) {
                onTrade(trade);
            } else if (msg instanceof MarketDataMessage.SecurityList list) {
                log.info("remote ORB security list recibida securities={}", list.getListSecuritiesCount());
            }
        } catch (Exception e) {
            log.error("remote ORB message error type={}", msg == null ? "null" : msg.getClass().getSimpleName(), e);
        }
    }

    private void onConnected() {
        MainApp.setRemoteClientConnected(true);
        NotificationCenter.get().publish(NotificationType.CONEXION, "ORB remoto",
                "conectado a " + MainApp.getRemoteClientHost());
        client.sendMessage(MarketDataMessage.SecurityListRequest.newBuilder()
                .setId("remote-security-list")
                .build());
        for (MarketDataMessage.Subscribe sub : configuredSubscriptions()) {
            register(sub);
            client.sendMessage(sub);
            log.info("remote ORB subscribe symbol='{}' id={}", sub.getSymbol(), sub.getId());
        }
    }

    private List<MarketDataMessage.Subscribe> configuredSubscriptions() {
        String configured = properties.getProperty("bootstrap.subscribe.symbols", "").trim();
        if (configured.isEmpty()) {
            return List.of();
        }
        Set<String> symbols = new LinkedHashSet<>();
        for (String token : configured.split("[,;\\r\\n]+")) {
            String symbol = token == null ? "" : token.trim();
            if (!symbol.isEmpty()) {
                symbols.add(symbol);
            }
        }
        List<MarketDataMessage.Subscribe> subscriptions = new ArrayList<>();
        int index = 1;
        for (String symbol : symbols) {
            subscriptions.add(MarketDataMessage.Subscribe.newBuilder()
                    .setSymbol(symbol)
                    .setId("remote-" + index++)
                    .setSecurityExchange(MainApp.securityExchange)
                    .setSettlType(RoutingMessage.SettlType.REGULAR)
                    .setTrade(true)
                    .setStatistic(true)
                    .setBook(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .build());
        }
        return subscriptions;
    }

    private void register(MarketDataMessage.Subscribe sub) {
        byId.put(sub.getId(), sub);
        topicById.put(sub.getId(), TopicGenerator.getTopicMKD(sub));
        MainApp.bookHasmap.putIfAbsent(topicById.get(sub.getId()), new BookSnapshot(topicById.get(sub.getId()), sub));
    }

    private BookSnapshot snapshotFor(String id, String symbol, MarketDataMessage.SecurityExchangeMarketData exchange,
                                     RoutingMessage.SettlType settlType) {
        MarketDataMessage.Subscribe sub = byId.computeIfAbsent(id, ignored -> MarketDataMessage.Subscribe.newBuilder()
                .setId(id)
                .setSymbol(symbol)
                .setSecurityExchange(exchange)
                .setSettlType(settlType)
                .setTrade(true)
                .setStatistic(true)
                .setBook(true)
                .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                .build());
        String topic = topicById.computeIfAbsent(id, ignored -> TopicGenerator.getTopicMKD(sub));
        return MainApp.bookHasmap.computeIfAbsent(topic, ignored -> new BookSnapshot(topic, sub));
    }

    private void onSnapshot(MarketDataMessage.Snapshot msg) {
        BookSnapshot bs = snapshotFor(msg.getId(), msg.getSymbol(), msg.getSecurityExchange(), msg.getSettlType());
        bs.getBid().clear();
        bs.getBid().addAll(msg.getBidsList());
        bs.getAsk().clear();
        bs.getAsk().addAll(msg.getAsksList());
        bs.getTrades().clear();
        bs.getTrades().addAll(msg.getTradesList());
        if (msg.hasStatistic()) {
            bs.setStatistic(msg.getStatistic().toBuilder());
            if (msg.getStatistic().hasOhlcv()) {
                bs.setOhlcv(msg.getStatistic().getOhlcv().toBuilder());
            }
        }
        bs.setReceivedSnapshot(true);
        markRemoteData();
    }

    private void onStatistic(MarketDataMessage.Statistic msg) {
        BookSnapshot bs = snapshotFor(msg.getId(), msg.getSymbol(), msg.getSecurityExchange(), msg.getSettlType());
        bs.setStatistic(msg.toBuilder());
        if (msg.hasOhlcv()) {
            bs.setOhlcv(msg.getOhlcv().toBuilder());
        }
        bs.setReceivedSnapshot(true);
        markRemoteData();
    }

    private void onIncrementalBook(MarketDataMessage.IncrementalBook msg) {
        BookSnapshot bs = snapshotFor(msg.getId(), msg.getSymbol(), msg.getSecurityExchange(), msg.getSettlType());
        if (msg.getBidsCount() > 0) {
            bs.getBid().clear();
            bs.getBid().addAll(msg.getBidsList());
        }
        if (msg.getAsksCount() > 0) {
            bs.getAsk().clear();
            bs.getAsk().addAll(msg.getAsksList());
        }
        bs.setReceivedSnapshot(true);
        markRemoteData();
    }

    private void onTrade(MarketDataMessage.Trade msg) {
        BookSnapshot bs = snapshotFor(msg.getId(), msg.getSymbol(), msg.getSecurityExchange(), msg.getSettlType());
        bs.getTrades().add(msg);
        if (bs.getTrades().size() > 2000) {
            bs.getTrades().removeFirst();
        }
        if (msg.getPrice() > 0) {
            bs.getStatistic().setLast(msg.getPrice());
        }
        bs.setReceivedSnapshot(true);
        markRemoteData();
    }

    private void markRemoteData() {
        MainApp.getBloombergTicks().increment();
        MainApp.markData();
    }
}
