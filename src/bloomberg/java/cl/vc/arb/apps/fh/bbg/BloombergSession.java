package cl.vc.arb.apps.fh.bbg;

import akka.actor.ActorRef;
import cl.vc.arb.apps.fh.ingest.BbgTick;
import cl.vc.arb.apps.fh.ingest.Bar;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import com.bloomberglp.blpapi.CorrelationID;
import com.bloomberglp.blpapi.Datetime;
import com.bloomberglp.blpapi.Element;
import com.bloomberglp.blpapi.Event;
import com.bloomberglp.blpapi.Message;
import com.bloomberglp.blpapi.Request;
import com.bloomberglp.blpapi.Schema;
import com.bloomberglp.blpapi.Service;
import com.bloomberglp.blpapi.Session;
import com.bloomberglp.blpapi.SessionOptions;
import com.bloomberglp.blpapi.Subscription;
import com.bloomberglp.blpapi.SubscriptionList;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
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
    private static final String REFDATA_SERVICE = "//blp/refdata";

    private final Properties properties;
    private Session session;
    private volatile boolean connected = false;

    // Sesion SINCRONA dedicada a requests historicos (//blp/refdata). Se crea perezosamente y se
    // reutiliza; una sola request a la vez (refLock). Separada de la sesion de streaming porque esta
    // tiene event-handler asincrono y nextEvent() no se puede usar ahi.
    private Session refSession;
    private final Object refLock = new Object();

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
        synchronized (refLock) {
            try {
                if (refSession != null) {
                    refSession.stop();
                }
            } catch (Exception ignore) {
                // best-effort
            }
            refSession = null;
        }
    }

    // ------------------------------------------------------------------ //
    //  Historico OHLCV para el grafico de velas (//blp/refdata)           //
    // ------------------------------------------------------------------ //

    @Override
    public List<Bar> history(String bbgSecurity, String interval, int points) {
        if (bbgSecurity == null || bbgSecurity.isBlank()) {
            return List.of();
        }
        int n = points <= 0 ? 120 : Math.min(points, 1000);
        try {
            synchronized (refLock) { // una request a la vez
                Session s = ensureRefSession();
                Service svc = s.getService(REFDATA_SERVICE);
                List<Bar> bars = "DAILY".equalsIgnoreCase(interval)
                        ? dailyHistory(s, svc, bbgSecurity, n)
                        : intradayBars(s, svc, bbgSecurity, parseIntSafe(interval, 1), n);
                log.info("bloomberg history security='{}' interval={} velas={}", bbgSecurity, interval, bars.size());
                return bars;
            }
        } catch (Throwable t) {
            log.warn("bloomberg history error security='{}' interval={}", bbgSecurity, interval, t);
            return List.of();
        }
    }

    private Session ensureRefSession() throws Exception {
        if (refSession != null) {
            return refSession;
        }
        String host = properties.getProperty("bloomberg.host", "localhost");
        int port = Integer.parseInt(properties.getProperty("bloomberg.port", "8194"));
        SessionOptions o = new SessionOptions();
        o.setServerHost(host);
        o.setServerPort(port);
        Session s = new Session(o); // sincrona (sin handler) -> se puede usar nextEvent()
        if (!s.start()) {
            throw new IllegalStateException("no se pudo iniciar la sesion refdata");
        }
        if (!s.openService(REFDATA_SERVICE)) {
            throw new IllegalStateException("no se pudo abrir " + REFDATA_SERVICE);
        }
        refSession = s;
        log.info("bloomberg refdata session OK {}:{} service={}", host, port, REFDATA_SERVICE);
        return s;
    }

    /** Velas diarias (fin de dia) via HistoricalDataRequest. */
    private List<Bar> dailyHistory(Session s, Service svc, String security, int points) throws Exception {
        Request req = svc.createRequest("HistoricalDataRequest");
        req.getElement("securities").appendValue(security);
        for (String f : new String[]{"PX_OPEN", "PX_HIGH", "PX_LOW", "PX_LAST", "PX_VOLUME"}) {
            req.getElement("fields").appendValue(f);
        }
        req.set("periodicitySelection", "DAILY");
        java.time.LocalDate today = java.time.LocalDate.now();
        java.time.LocalDate start = today.minusDays((long) points * 2L + 10L); // colchon por fines de semana/feriados
        req.set("startDate", ymd(start));
        req.set("endDate", ymd(today));
        req.set("maxDataPoints", points);

        s.sendRequest(req, new CorrelationID(900_000L));
        List<Bar> bars = new ArrayList<>();
        drain(s, ev -> {
            for (Message m : ev) {
                if (!m.hasElement("securityData")) {
                    continue;
                }
                Element sd = m.getElement("securityData");
                if (sd.hasElement("securityError")) {
                    log.warn("bloomberg histo securityError {}", sd.getElement("securityError"));
                    continue;
                }
                Element fda = sd.getElement("fieldData");
                int cnt = fda.numValues();
                for (int i = 0; i < cnt; i++) {
                    Element fd = fda.getValueAsElement(i);
                    long t = fd.hasElement("date") ? toMillis(fd.getElementAsDate("date")) : 0L;
                    bars.add(new Bar(t, f(fd, "PX_OPEN"), f(fd, "PX_HIGH"), f(fd, "PX_LOW"),
                            f(fd, "PX_LAST"), f(fd, "PX_VOLUME")));
                }
            }
        });
        return bars;
    }

    /** Velas intradia via IntradayBarRequest (eventType TRADE). */
    private List<Bar> intradayBars(Session s, Service svc, String security, int minutes, int points) throws Exception {
        int interval = Math.max(1, minutes);
        Request req = svc.createRequest("IntradayBarRequest");
        req.set("security", security);
        req.set("eventType", "TRADE");
        req.set("interval", interval);
        int lookbackDays = interval <= 1 ? 3 : interval <= 5 ? 7 : 20;
        req.set("startDateTime", toDatetime(java.time.LocalDateTime.now().minusDays(lookbackDays)));
        req.set("endDateTime", toDatetime(java.time.LocalDateTime.now().plusDays(1)));
        req.set("gapFillInitialBar", true);

        s.sendRequest(req, new CorrelationID(900_001L));
        List<Bar> bars = new ArrayList<>();
        drain(s, ev -> {
            for (Message m : ev) {
                if (!m.hasElement("barData")) {
                    continue;
                }
                Element ticks = m.getElement("barData").getElement("barTickData");
                int cnt = ticks.numValues();
                for (int i = 0; i < cnt; i++) {
                    Element b = ticks.getValueAsElement(i);
                    long t = b.hasElement("time") ? toMillis(b.getElementAsDate("time")) : 0L;
                    bars.add(new Bar(t, f(b, "open"), f(b, "high"), f(b, "low"),
                            f(b, "close"), f(b, "volume")));
                }
            }
        });
        if (bars.size() > points) {
            return new ArrayList<>(bars.subList(bars.size() - points, bars.size()));
        }
        return bars;
    }

    /** Bombea eventos de la sesion sincrona hasta el RESPONSE final (o timeout de seguridad). */
    private void drain(Session s, java.util.function.Consumer<Event> onData) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000L;
        boolean done = false;
        while (!done && System.currentTimeMillis() < deadline) {
            Event ev = s.nextEvent(1000);
            int et = ev.eventType().intValue();
            if (et == Event.EventType.Constants.TIMEOUT) {
                continue;
            }
            if (et == Event.EventType.Constants.RESPONSE || et == Event.EventType.Constants.PARTIAL_RESPONSE) {
                onData.accept(ev);
            }
            if (et == Event.EventType.Constants.RESPONSE) {
                done = true;
            }
        }
    }

    private static double f(Element e, String field) {
        try {
            return e.hasElement(field) && !e.getElement(field).isNull() ? e.getElementAsFloat64(field) : Double.NaN;
        } catch (Exception ex) {
            return Double.NaN;
        }
    }

    private static int parseIntSafe(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return def;
        }
    }

    private static String ymd(java.time.LocalDate d) {
        return String.format("%04d%02d%02d", d.getYear(), d.getMonthValue(), d.getDayOfMonth());
    }

    private static Datetime toDatetime(java.time.LocalDateTime t) {
        return new Datetime(t.getYear(), t.getMonthValue(), t.getDayOfMonth(),
                t.getHour(), t.getMinute(), t.getSecond(), 0);
    }

    private static long toMillis(Datetime dt) {
        try {
            int y = dt.year(), mo = dt.month(), d = dt.dayOfMonth();
            int h = 0, mi = 0;
            try { h = dt.hour(); mi = dt.minute(); } catch (Throwable ignore) { /* solo fecha */ }
            java.util.Calendar c = java.util.Calendar.getInstance();
            c.clear();
            c.set(y, mo - 1, d, h, mi, 0);
            return c.getTimeInMillis();
        } catch (Throwable t) {
            return 0L;
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
