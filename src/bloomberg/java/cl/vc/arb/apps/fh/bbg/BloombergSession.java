package cl.vc.arb.apps.fh.bbg;

import akka.actor.ActorRef;
import cl.vc.arb.apps.fh.ingest.BbgTick;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Schema;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pasarela de ingesta Bloomberg via BLPAPI Desktop API (DAPI), servicio {@code //blp/mktdata}
 * (top-of-book). Abre UNA sesion contra la Terminal local ({@code localhost:8194}), suscribe cada
 * security una sola vez (dedup) y, por cada tick, extrae los campos numericos y los entrega como
 * {@link BbgTick} a los actores de topic enganchados (fan-out).
 *
 * <p><b>Solo se compila con el perfil Maven {@code -Pbloomberg}</b> (requiere blpapi.jar). En runtime
 * necesita ademas la libreria nativa {@code blpapi3_64.dll} en el {@code PATH}/{@code java.library.path}
 * y una Terminal Bloomberg corriendo. El nucleo la carga por reflexion (ver SellSideManager).</p>
 *
 * <p>Nota: la API de blpapi varia levemente entre versiones; si tu blpapi.jar difiere, ajusta las
 * firmas (Event iterable, Subscription, datatype) a tu version del SDK.</p>
 */
@Slf4j
public class BloombergSession implements BloombergGateway {

    private static final String MKTDATA_SERVICE = "//blp/mktdata";

    private final Properties properties;
    private Session session;
    private volatile boolean connected = false;

    // Una suscripcion BLPAPI por security, con fan-out a N actores de topic.
    private final Map<String, Long> cidBySecurity = new ConcurrentHashMap<>();
    private final Map<Long, String> securityByCid = new ConcurrentHashMap<>();
    private final Map<Long, Set<ActorRef>> actorsByCid = new ConcurrentHashMap<>();
    private final Map<ActorRef, Long> cidByActor = new ConcurrentHashMap<>();
    private final Map<Long, List<String>> fieldsByCid = new ConcurrentHashMap<>();
    private final AtomicLong cidSeq = new AtomicLong(1);

    public BloombergSession(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void start() {
        try {
            String host = properties.getProperty("bloomberg.host", "localhost");
            int port = Integer.parseInt(properties.getProperty("bloomberg.port", "8194"));

            SessionOptions options = new SessionOptions();
            options.setServerHost(host);
            options.setServerPort(port);

            session = new Session(options, this::processEvent);

            if (!session.start()) {
                log.error("bloomberg: no se pudo iniciar la sesion BLPAPI {}:{} (¿Terminal corriendo?)", host, port);
                return;
            }
            if (!session.openService(MKTDATA_SERVICE)) {
                log.error("bloomberg: no se pudo abrir el servicio {}", MKTDATA_SERVICE);
                return;
            }
            connected = true;
            log.info("bloomberg session OK {}:{} service={}", host, port, MKTDATA_SERVICE);
        } catch (Throwable t) {
            // Throwable: tambien atrapa UnsatisfiedLinkError si falta blpapi3_64.dll.
            log.error("bloomberg start error (¿falta blpapi3_64.dll en el PATH?)", t);
        }
    }

    @Override
    public synchronized void subscribe(String topic, String bbgSecurity, List<String> fields, ActorRef topicActor) {
        try {
            if (session == null) {
                log.warn("bloomberg subscribe ignorado (sesion no iniciada) security={}", bbgSecurity);
                return;
            }

            Long cid = cidBySecurity.get(bbgSecurity);
            if (cid != null) {
                actorsByCid.computeIfAbsent(cid, k -> ConcurrentHashMap.newKeySet()).add(topicActor);
                cidByActor.put(topicActor, cid);
                log.info("bloomberg subscribe REUSE security='{}' cid={} fanout={}", bbgSecurity, cid, actorsByCid.get(cid).size());
                return;
            }

            long newCid = cidSeq.getAndIncrement();
            cidBySecurity.put(bbgSecurity, newCid);
            securityByCid.put(newCid, bbgSecurity);
            fieldsByCid.put(newCid, fields);
            actorsByCid.computeIfAbsent(newCid, k -> ConcurrentHashMap.newKeySet()).add(topicActor);
            cidByActor.put(topicActor, newCid);

            SubscriptionList subscriptions = new SubscriptionList();
            subscriptions.add(new Subscription(bbgSecurity, fields, new CorrelationID(newCid)));
            session.subscribe(subscriptions);
            log.info("bloomberg subscribe NEW security='{}' cid={} fields={}", bbgSecurity, newCid, fields);

        } catch (Exception e) {
            log.error("bloomberg subscribe error security='{}'", bbgSecurity, e);
        }
    }

    @Override
    public synchronized void unsubscribe(String topic, ActorRef topicActor) {
        try {
            Long cid = cidByActor.remove(topicActor);
            if (cid == null) {
                return;
            }
            Set<ActorRef> set = actorsByCid.get(cid);
            if (set != null) {
                set.remove(topicActor);
            }
            if (set == null || set.isEmpty()) {
                String sec = securityByCid.remove(cid);
                List<String> fields = fieldsByCid.remove(cid);
                actorsByCid.remove(cid);
                if (sec != null) {
                    cidBySecurity.remove(sec);
                }
                try {
                    if (session != null && sec != null) {
                        SubscriptionList sl = new SubscriptionList();
                        sl.add(new Subscription(sec, fields == null ? List.of() : fields, new CorrelationID(cid)));
                        session.unsubscribe(sl);
                    }
                } catch (Exception ex) {
                    log.warn("bloomberg unsubscribe BLPAPI error security='{}'", sec, ex);
                }
                log.info("bloomberg unsubscribe security='{}' cid={} (sin clientes)", sec, cid);
            }
        } catch (Exception e) {
            log.error("bloomberg unsubscribe error topic={}", topic, e);
        }
    }

    @Override
    public boolean isConnected() {
        return connected && session != null;
    }

    @Override
    public void stop() {
        connected = false;
        try {
            if (session != null) {
                session.stop();
            }
        } catch (Exception ignore) {
            // best-effort
        }
    }

    // ------------------------------------------------------------------ //
    //  Event handler (corre en el hilo de BLPAPI)                          //
    // ------------------------------------------------------------------ //

    private void processEvent(Event event, Session session) {
        try {
            switch (event.eventType().intValue()) {
                case Event.EventType.Constants.SUBSCRIPTION_DATA:
                    for (Message msg : event) {
                        dispatch(msg);
                    }
                    break;
                case Event.EventType.Constants.SUBSCRIPTION_STATUS:
                case Event.EventType.Constants.SESSION_STATUS:
                case Event.EventType.Constants.SERVICE_STATUS:
                    for (Message msg : event) {
                        log.info("bloomberg {} -> {}", event.eventType(), msg.messageType());
                    }
                    break;
                default:
                    // ignorar otros tipos
            }
        } catch (Exception e) {
            log.error("bloomberg processEvent error", e);
        }
    }

    private void dispatch(Message msg) {
        long cid = msg.correlationID().value();
        Set<ActorRef> actors = actorsByCid.get(cid);
        if (actors == null || actors.isEmpty()) {
            return;
        }
        Map<String, Double> nums = new HashMap<>();
        String eventType = extractFields(msg, nums);
        if (nums.isEmpty() && eventType == null) {
            return;
        }
        BbgTick tick = new BbgTick(securityByCid.get(cid), nums, eventType);
        for (ActorRef a : actors) {
            a.tell(tick, ActorRef.noSender());
        }
    }

    /** Extrae los campos numericos presentes en el mensaje; devuelve MKTDATA_EVENT_TYPE si vino. */
    private String extractFields(Message msg, Map<String, Double> out) {
        String eventType = null;
        try {
            Element data = msg.asElement();
            int n = data.numElements();
            for (int i = 0; i < n; i++) {
                Element e = data.getElement(i);
                if (e.isNull()) {
                    continue;
                }
                String name = e.name().toString();
                if ("MKTDATA_EVENT_TYPE".equals(name) || "MKTDATA_EVENT_SUBTYPE".equals(name)) {
                    try {
                        eventType = e.getValueAsString();
                    } catch (Exception ignore) {
                        // no string-convertible
                    }
                    continue;
                }
                Schema.Datatype dt = e.datatype();
                if (dt == Schema.Datatype.FLOAT64 || dt == Schema.Datatype.FLOAT32
                        || dt == Schema.Datatype.INT32 || dt == Schema.Datatype.INT64) {
                    try {
                        out.put(name, e.getValueAsFloat64());
                    } catch (Exception ignore) {
                        // campo no numerico utilizable
                    }
                }
            }
        } catch (Exception e) {
            log.debug("bloomberg extractFields error", e);
        }
        return eventType;
    }
}
