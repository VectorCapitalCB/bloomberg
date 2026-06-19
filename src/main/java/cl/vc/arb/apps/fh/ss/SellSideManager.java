package cl.vc.arb.apps.fh.ss;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import cl.vc.module.protocolbuff.akka.MessageEventBus;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Punto de entrada de suscripciones del ORB. Recibe los {@link Subscribe} que arman los clientes
 * (via ClientManager) y crea/reutiliza un actor de topic {@link InversorBloombergToProto} por
 * instrumento. En arranque, inicia la ingesta segun {@code securityExchange}:
 * <ul>
 *   <li>{@code simulador=yes} -> precarga la security list desde los precios de referencia.</li>
 *   <li>{@code BLOOMBERG_MKD} -> abre la sesion BLPAPI (cargada por reflexion; solo existe con -Pbloomberg).</li>
 * </ul>
 */
@Slf4j
public class SellSideManager extends AbstractActor {

    private final MessageEventBus eventBus;
    private static final AtomicBoolean atomicBool = new AtomicBoolean(false);
    private static final Map<String, ActorRef> mapActor = new ConcurrentHashMap<>();
    private static final Map<ActorRef, String> actorTopic = new ConcurrentHashMap<>();
    private static final Map<String, MarketDataMessage.Subscribe> symbolsubscribe = new ConcurrentHashMap<>();

    private final Properties properties;

    public static Props props(Properties properties, MessageEventBus eventBus) {
        return Props.create(SellSideManager.class, properties, eventBus);
    }

    public SellSideManager(Properties properties, MessageEventBus eventBus) {
        this.eventBus = eventBus;
        this.properties = properties;
    }

    @Override
    public void preStart() {
        if (!atomicBool.compareAndSet(false, true)) {
            return;
        }
        boolean isSimulator = "yes".equalsIgnoreCase(properties.getProperty("simulador", "no"));

        if (isSimulator) {
            // Precarga la security list desde los precios de referencia para que los clientes
            // puedan pedirla sin proveedor en vivo.
            MainApp.getSimulatorPrices().forEach((symbol, ref) -> {
                MarketDataMessage.Security security = MarketDataMessage.Security.newBuilder()
                        .setSymbol(symbol)
                        .setSecurityExchange(MainApp.securityExchange)
                        .setCurrency("CLP")
                        .setSecurityType("CS")
                        .build();
                String key = symbol + MainApp.securityExchange.name();
                MainApp.securityListMaps.put(key, security);
                MainApp.securityList.addListSecurities(security);
            });
            log.info("sell side simulator mode populated {} securities", MainApp.getSimulatorPrices().size());

        } else if (MainApp.securityExchange == MarketDataMessage.SecurityExchangeMarketData.BLOOMBERG_MKD) {
            startBloomberg();
        } else {
            log.warn("sell side: securityExchange={} sin ingesta configurada (usa BLOOMBERG_MKD o simulador=yes)",
                    MainApp.securityExchange);
        }
    }

    /**
     * Carga la pasarela BLPAPI por reflexion. La implementacion {@code cl.vc.arb.apps.fh.bbg.BloombergSession}
     * solo se compila con el perfil Maven {@code -Pbloomberg} (necesita blpapi.jar). Asi el build por
     * defecto (simulador) no requiere blpapi.
     */
    private void startBloomberg() {
        try {
            Class<?> c = Class.forName("cl.vc.arb.apps.fh.bbg.BloombergSession");
            BloombergGateway gw = (BloombergGateway) c.getDeclaredConstructor(Properties.class).newInstance(properties);
            MainApp.setBloombergGateway(gw);
            gw.start();
            log.info("bloomberg ingest iniciada gateway={}", c.getName());
        } catch (ClassNotFoundException e) {
            log.error("Bloomberg ingest NO incluida en este build. Recompila con:  mvn -Pbloomberg -DskipTests package " +
                    "(requiere blpapi.jar instalado en .m2). securityExchange={}", MainApp.securityExchange);
        } catch (Exception e) {
            log.error("no se pudo iniciar la ingesta Bloomberg", e);
        }
    }

    @Override
    public void postStop() {
        try {
            BloombergGateway gw = MainApp.getBloombergGateway();
            if (gw != null) {
                gw.stop();
            }
            eventBus.unsubscribe(getSelf());
        } catch (Exception ex) {
            log.error("Cannot stop SellSide Manager...", ex);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Subscribe.class, this::onMarketDataRequest)
                .match(Terminated.class, this::onChildTerminated)
                .build();
    }

    @Data
    public static class Subscribe {
        private final MarketDataMessage.Subscribe subscribe;
        private final ActorRef actorRef;
    }

    private void onMarketDataRequest(Subscribe request) {
        try {
            if (request.getSubscribe().getId().isEmpty()) {
                return;
            }

            String id = TopicGenerator.getTopicMKD(request.getSubscribe());
            log.debug("market data request clientSubId={} topic={} cachedActor={}",
                    request.getSubscribe().getId(), id, mapActor.containsKey(id));

            ActorRef actorRef = mapActor.get(id);
            if (actorRef != null && actorRef.isTerminated()) {
                mapActor.remove(id, actorRef);
                actorTopic.remove(actorRef);
                symbolsubscribe.remove(id);
                actorRef = null;
            }

            if (actorRef != null) {
                log.info("reusing market data actor topic={} actor={} for clientSubId={}",
                        id, actorRef, request.getSubscribe().getId());
                actorRef.tell(new InversorBloombergToProto.SendSnapshot(request.getSubscribe(), request.getActorRef()),
                        ActorRef.noSender());
            } else {
                symbolsubscribe.put(id, request.getSubscribe());
                ActorRef ref = getContext().actorOf(InversorBloombergToProto.props(request.getSubscribe(), eventBus));
                ActorRef existing = mapActor.putIfAbsent(id, ref);
                if (existing != null) {
                    getContext().stop(ref);
                    existing.tell(new InversorBloombergToProto.SendSnapshot(request.getSubscribe(), request.getActorRef()),
                            ActorRef.noSender());
                    log.info("reused concurrently created market data actor topic={} actor={} clientSubId={}",
                            id, existing, request.getSubscribe().getId());
                    return;
                }
                actorTopic.put(ref, id);
                getContext().watch(ref);
                log.info("created market data actor topic={} actor={} clientSubId={}",
                        id, ref, request.getSubscribe().getId());
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onChildTerminated(Terminated msg) {
        ActorRef child = msg.getActor();
        String topic = actorTopic.remove(child);
        if (topic == null) {
            return;
        }
        mapActor.remove(topic, child);
        symbolsubscribe.remove(topic);
        log.info("market data actor terminated topic={} actor={} remainingActors={}",
                topic, child, mapActor.size());
    }
}
