package fh.test;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FHLoadTest {
    private static final String DEFAULT_PROPERTIES_PATH =
            "E:\\VC-GITHUB\\ORB-BCS\\resources-bcs\\fh.sellside.properties";
    private static final String DEFAULT_SYMBOL = "SQM-B";
    private static final int DEFAULT_SUBSCRIPTIONS = 10000;
    private static final int DEFAULT_DELAY_MS = 0;

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        String propertiesPath = args.length > 0 ? args[0] : DEFAULT_PROPERTIES_PATH;
        try (FileInputStream fis = new FileInputStream(propertiesPath)) {
            properties.load(fis);
        }

        String configuredHost = properties.getProperty("server.host");
        if (configuredHost == null || configuredHost.isBlank()) {
            throw new IllegalArgumentException("Missing server.host in " + propertiesPath);
        }
        String host = normalizeClientHost(configuredHost);

        String symbol = args.length > 1 ? args[1] : DEFAULT_SYMBOL;
        int subscriptions = args.length > 2 ? Integer.parseInt(args[2]) : DEFAULT_SUBSCRIPTIONS;
        int delayMs = args.length > 3 ? Integer.parseInt(args[3]) : DEFAULT_DELAY_MS;
        MarketDataMessage.SecurityExchangeMarketData exchange =
                MarketDataMessage.SecurityExchangeMarketData.valueOf(properties.getProperty("securityExchange"));

        System.out.printf(
                "Starting load test with properties=%s host=%s symbol=%s subscriptions=%d delayMs=%d%n",
                propertiesPath, host, symbol, subscriptions, delayMs);

        ActorSystem system = ActorSystem.create("fh-load-test");
        CountDownLatch connected = new CountDownLatch(1);
        StatsActor.reset();
        ActorRef statsActor = system.actorOf(StatsActor.props(connected), "load-stats");

        NettyProtobufClient client = new NettyProtobufClient(
                host,
                statsActor,
                "logs/fh-load-test",
                exchange.name(),
                NotificationMessage.Component.ORB_BCS,
                false,
                "fh-load-test");

        client.start();

        if (!connected.await(20, TimeUnit.SECONDS)) {
            throw new IllegalStateException("Client did not connect to " + host + " within 20 seconds");
        }

        client.sendMessage(MarketDataMessage.SecurityListRequest.newBuilder()
                .setId("security-list-" + IDGenerator.getID())
                .build());

        Thread.sleep(1000);

        Instant start = Instant.now();
        for (int i = 0; i < subscriptions; i++) {
            MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setId("load-" + i + "-" + IDGenerator.getID())
                    .setSymbol(symbol)
                    .setBook(true)
                    .setTrade(true)
                    .setStatistic(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .setSettlType(RoutingMessage.SettlType.T2)
                    .setSecurityExchange(exchange)
                    .build();
            client.sendMessage(subscribe);
            if (delayMs > 0) {
                Thread.sleep(delayMs);
            }
        }

        Duration sendDuration = Duration.between(start, Instant.now());
        System.out.printf("Sent %,d subscriptions for %s in %d ms%n",
                subscriptions, symbol, sendDuration.toMillis());

        long reportUntil = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < reportUntil) {
            Thread.sleep(5_000L);
            System.out.printf(
                    "stats snapshots=%d incrementals=%d trades=%d statistics=%d rejects=%d lastMessageAgoMs=%d%n",
                    StatsActor.snapshots.get(),
                    StatsActor.incrementals.get(),
                    StatsActor.trades.get(),
                    StatsActor.statistics.get(),
                    StatsActor.rejects.get(),
                    StatsActor.lastMessageAgoMs());
        }

        client.stopClient();
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    public static class StatsActor extends AbstractActor {
        private static final Logger log = LoggerFactory.getLogger(StatsActor.class);
        private static final AtomicInteger snapshots = new AtomicInteger();
        private static final AtomicInteger incrementals = new AtomicInteger();
        private static final AtomicInteger trades = new AtomicInteger();
        private static final AtomicInteger statistics = new AtomicInteger();
        private static final AtomicInteger rejects = new AtomicInteger();
        private static final AtomicLong lastMessageTs = new AtomicLong();

        private final CountDownLatch connected;

        public StatsActor(CountDownLatch connected) {
            this.connected = connected;
        }

        public static Props props(CountDownLatch connected) {
            return Props.create(StatsActor.class, connected);
        }

        public static void reset() {
            snapshots.set(0);
            incrementals.set(0);
            trades.set(0);
            statistics.set(0);
            rejects.set(0);
            lastMessageTs.set(0);
        }

        public static long lastMessageAgoMs() {
            long ts = lastMessageTs.get();
            return ts == 0 ? -1 : System.currentTimeMillis() - ts;
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(TransportingObjects.class, this::onMessage)
                    .build();
        }

        private void onMessage(TransportingObjects conn) {
            Object message = conn.getMessage();
            lastMessageTs.set(System.currentTimeMillis());

            if (message instanceof SessionsMessage.Connect) {
                connected.countDown();
                log.info("load test connected: {}", message);
                return;
            }

            if (message instanceof SessionsMessage.Disconnect) {
                log.warn("load test disconnected: {}", message);
                return;
            }

            if (message instanceof MarketDataMessage.Snapshot) {
                snapshots.incrementAndGet();
                return;
            }

            if (message instanceof MarketDataMessage.IncrementalBook) {
                incrementals.incrementAndGet();
                return;
            }

            if (message instanceof MarketDataMessage.Trade) {
                trades.incrementAndGet();
                return;
            }

            if (message instanceof MarketDataMessage.Statistic) {
                statistics.incrementAndGet();
                return;
            }

            if (message instanceof MarketDataMessage.Rejected) {
                rejects.incrementAndGet();
                log.warn("rejected: {}", message);
            }
        }
    }

    private static String normalizeClientHost(String configuredHost) {
        String[] parts = configuredHost.split(":", 2);
        if (parts.length != 2) {
            return configuredHost;
        }

        String hostname = parts[0].trim();
        if ("0.0.0.0".equals(hostname) || "::".equals(hostname) || "[::]".equals(hostname)) {
            hostname = "127.0.0.1";
        }
        return hostname + ":" + parts[1].trim();
    }
}
