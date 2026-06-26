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

            securityExchange = MarketDataMessage.SecurityExchangeMarketData.valueOf(properties.getProperty("securityExchange"));

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

            ensureDesktopShortcut();

            start();
            startDiagnostics();

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    /** Crea el acceso directo en el escritorio (icono naranja) si no existe. Solo corre como app instalada. */
    private static void ensureDesktopShortcut() {
        try {
            String appPath = System.getProperty("jpackage.app-path");
            if (appPath == null || appPath.isBlank()) return;
            String ps = "$d=[Environment]::GetFolderPath('Desktop');"
                    + "$p=Join-Path $d 'ORB-BLOOMBERG.lnk';"
                    + "if(-not (Test-Path $p)){"
                    + "$w=New-Object -ComObject WScript.Shell;"
                    + "$s=$w.CreateShortcut($p);"
                    + "$s.TargetPath='" + appPath + "';"
                    + "$s.IconLocation='" + appPath + ",0';"
                    + "$s.WorkingDirectory=[System.IO.Path]::GetDirectoryName('" + appPath + "');"
                    + "$s.Save()}";
            new ProcessBuilder("powershell", "-WindowStyle", "Hidden", "-NoProfile", "-Command", ps).start();
            log.info("acceso directo del escritorio verificado");
        } catch (Throwable t) {
            log.debug("no se pudo crear el acceso directo: {}", t.getMessage());
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

}
