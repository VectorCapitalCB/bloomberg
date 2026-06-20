package cl.vc.arb.apps.fh.admin;

import akka.actor.ActorRef;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.ingest.BloombergGateway;
import cl.vc.arb.apps.fh.ss.SellSideManager;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Panel de administracion web embebido (sin dependencias: usa com.sun.net.httpserver del JDK).
 * Solo LECTURA sobre el estado que el app ya mantiene -> no toca la ruta de datos de baja latencia.
 * Sirve el dashboard ({@code /admin.html}) y una API JSON para monitoreo + acciones de admin.
 */
@Slf4j
public class AdminServer {

    private final int port;
    private HttpServer server;

    public AdminServer(int port) {
        this.port = port;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", this::serveDashboard);
        server.createContext("/api/status", ex -> json(ex, statusJson()));
        server.createContext("/api/metrics", ex -> json(ex, metricsJson()));
        server.createContext("/api/securities", ex -> json(ex, securitiesJson()));
        server.createContext("/api/log", this::serveLog);
        server.createContext("/api/reconnect", this::actionReconnect);
        server.createContext("/api/ingest", this::actionIngest);
        server.createContext("/api/subscribe", this::actionSubscribe);
        server.createContext("/api/unsubscribe", this::actionUnsubscribe);
        server.setExecutor(Executors.newFixedThreadPool(4));
        server.start();
        log.info("admin panel arriba -> http://0.0.0.0:{}/", port);
    }

    public void stop() {
        if (server != null) server.stop(0);
    }

    // ------------------------------------------------------------------ //
    //  Dashboard                                                          //
    // ------------------------------------------------------------------ //

    private void serveDashboard(HttpExchange ex) throws IOException {
        try (InputStream in = AdminServer.class.getResourceAsStream("/admin.html")) {
            if (in == null) {
                send(ex, 404, "text/plain", "admin.html no encontrado".getBytes(StandardCharsets.UTF_8));
                return;
            }
            byte[] body = in.readAllBytes();
            send(ex, 200, "text/html; charset=utf-8", body);
        }
    }

    // ------------------------------------------------------------------ //
    //  API (read)                                                         //
    // ------------------------------------------------------------------ //

    private String statusJson() {
        BloombergGateway gw = MainApp.getBloombergGateway();
        boolean simulador = "yes".equalsIgnoreCase(MainApp.getProperties().getProperty("simulador", "no"));
        boolean connected = simulador || (gw != null && gw.isConnected());
        long up = System.currentTimeMillis() - MainApp.getStartTimeMs();
        StringBuilder sb = new StringBuilder("{");
        kv(sb, "exchange", MainApp.securityExchange != null ? MainApp.securityExchange.name() : "?", true);
        kvBool(sb, "connected", connected);
        kvBool(sb, "simulador", simulador);
        kv(sb, "host", MainApp.getProperties().getProperty("bloomberg.host", "localhost"), true);
        kv(sb, "port", MainApp.getProperties().getProperty("bloomberg.port", "8194"), true);
        kvNum(sb, "uptimeMs", up);
        String serverHost = MainApp.getProperties().getProperty("server.host", "");
        String nettyPort = serverHost.contains(":") ? serverHost.substring(serverHost.indexOf(':') + 1) : serverHost;
        kv(sb, "nettyPort", nettyPort, true);
        kv(sb, "nettyIp", localIp(), false);
        sb.append("}");
        return sb.toString();
    }

    private String metricsJson() {
        StringBuilder sb = new StringBuilder("{");
        kvNum(sb, "ticks", MainApp.getBloombergTicks().sum());
        kvNum(sb, "subscribers", MainApp.getActiveTopicSubscribers().sum());
        kvNum(sb, "papeles", MainApp.bookHasmap.size());
        kvNum(sb, "topicDistributors", MainApp.getTopicDistributorMaps().size());
        kvNum(sb, "outboundWrites", MainApp.getOutboundWrites().sum());
        kvNum(sb, "outboundSkipped", MainApp.getOutboundSkipped().sum());
        kvNum(sb, "outboundEvicted", MainApp.getOutboundEvicted().sum());
        kvNumLast(sb, "eventBusQueue", MainApp.queued());
        sb.append("}");
        return sb.toString();
    }

    private String securitiesJson() {
        List<String> rows = new ArrayList<>();
        for (Map.Entry<String, BookSnapshot> e : MainApp.bookHasmap.entrySet()) {
            BookSnapshot bs = e.getValue();
            if (bs == null) continue;
            MarketDataMessage.Statistic.Builder st = bs.getStatistic();
            StringBuilder r = new StringBuilder("{");
            kv(r, "topic", e.getKey(), true);
            kv(r, "symbol", bs.getSymbol(), true);
            kvF(r, "bid", st.getBidPx());
            kvF(r, "bidSize", st.getBidQty());
            kvF(r, "ask", st.getAskPx());
            kvF(r, "askSize", st.getAskQty());
            kvF(r, "last", st.getLast());
            kvF(r, "volume", st.getVolume());
            kvBoolLast(r, "ready", Boolean.TRUE.equals(bs.getReceivedSnapshot()));
            r.append("}");
            rows.add(r.toString());
        }
        return "[" + String.join(",", rows) + "]";
    }

    private void serveLog(HttpExchange ex) throws IOException {
        int lines = paramInt(ex, "lines", 250);
        String dir = System.getProperty("log.dir", MainApp.getProperties().getProperty("path.logs", "logs"));
        Path log = Paths.get(dir, "orb-bloomberg.log");
        String body;
        if (Files.exists(log)) {
            body = tail(log, lines, 256 * 1024);
        } else {
            body = "(sin log todavia en " + log + ")";
        }
        send(ex, 200, "text/plain; charset=utf-8", body.getBytes(StandardCharsets.UTF_8));
    }

    /** Lee SOLO los ultimos {@code maxBytes} del archivo (no carga todo el log) y devuelve las ultimas {@code lines} lineas. */
    private static String tail(Path file, int lines, int maxBytes) throws IOException {
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file.toFile(), "r")) {
            long len = raf.length();
            long start = Math.max(0, len - maxBytes);
            raf.seek(start);
            byte[] buf = new byte[(int) (len - start)];
            raf.readFully(buf);
            String text = new String(buf, StandardCharsets.UTF_8);
            if (start > 0) {
                int nl = text.indexOf('\n');
                if (nl >= 0) text = text.substring(nl + 1);
            }
            String[] arr = text.split("\n", -1);
            int from = Math.max(0, arr.length - lines);
            StringBuilder sb = new StringBuilder();
            for (int i = from; i < arr.length; i++) {
                if (i > from) sb.append('\n');
                sb.append(arr[i]);
            }
            return sb.toString();
        }
    }

    // ------------------------------------------------------------------ //
    //  API (actions)                                                      //
    // ------------------------------------------------------------------ //

    private void actionReconnect(HttpExchange ex) throws IOException {
        BloombergGateway gw = MainApp.getBloombergGateway();
        if (gw == null) {
            json(ex, "{\"ok\":false,\"msg\":\"sin ingesta Bloomberg (¿simulador?)\"}");
            return;
        }
        try {
            gw.stop();
            gw.start();
            log.info("admin: reconexion Bloomberg solicitada");
            json(ex, "{\"ok\":true,\"msg\":\"reconectando\"}");
        } catch (Exception e) {
            json(ex, "{\"ok\":false,\"msg\":\"" + esc(e.getMessage()) + "\"}");
        }
    }

    private void actionIngest(HttpExchange ex) throws IOException {
        String op = param(ex, "op", "");
        BloombergGateway gw = MainApp.getBloombergGateway();
        if (gw == null) {
            json(ex, "{\"ok\":false,\"msg\":\"sin ingesta Bloomberg\"}");
            return;
        }
        try {
            if ("stop".equals(op)) {
                gw.stop();
                log.info("admin: ingesta DETENIDA");
            } else if ("start".equals(op)) {
                gw.start();
                log.info("admin: ingesta INICIADA");
            }
            json(ex, "{\"ok\":true,\"msg\":\"" + esc(op) + "\"}");
        } catch (Exception e) {
            json(ex, "{\"ok\":false,\"msg\":\"" + esc(e.getMessage()) + "\"}");
        }
    }

    private void actionSubscribe(HttpExchange ex) throws IOException {
        String symbol = param(ex, "symbol", "").trim();
        if (symbol.isEmpty()) {
            json(ex, "{\"ok\":false,\"msg\":\"symbol vacio\"}");
            return;
        }
        try {
            MarketDataMessage.Subscribe sub = MarketDataMessage.Subscribe.newBuilder()
                    .setSymbol(symbol)
                    .setId("admin-" + symbol)
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BLOOMBERG_MKD)
                    .setSettlType(RoutingMessage.SettlType.REGULAR)
                    .setTrade(true).setStatistic(true).setBook(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .build();
            MainApp.getSellSideManager().tell(new SellSideManager.Subscribe(sub, ActorRef.noSender()), ActorRef.noSender());
            log.info("admin: suscripcion manual symbol='{}'", symbol);
            json(ex, "{\"ok\":true,\"msg\":\"suscrito " + esc(symbol) + "\"}");
        } catch (Exception e) {
            json(ex, "{\"ok\":false,\"msg\":\"" + esc(e.getMessage()) + "\"}");
        }
    }

    private void actionUnsubscribe(HttpExchange ex) throws IOException {
        String topic = param(ex, "topic", "").trim();
        if (topic.isEmpty()) {
            json(ex, "{\"ok\":false,\"msg\":\"topic vacio\"}");
            return;
        }
        try {
            MainApp.getSellSideManager().tell(new SellSideManager.AdminUnsub(topic), ActorRef.noSender());
            MainApp.bookHasmap.remove(topic);
            log.info("admin: desuscripcion manual topic='{}'", topic);
            json(ex, "{\"ok\":true,\"msg\":\"desuscrito\"}");
        } catch (Exception e) {
            json(ex, "{\"ok\":false,\"msg\":\"" + esc(e.getMessage()) + "\"}");
        }
    }

    // ------------------------------------------------------------------ //
    //  helpers                                                            //
    // ------------------------------------------------------------------ //

    private void json(HttpExchange ex, String body) throws IOException {
        send(ex, 200, "application/json; charset=utf-8", body.getBytes(StandardCharsets.UTF_8));
    }

    private void send(HttpExchange ex, int code, String ctype, byte[] body) throws IOException {
        ex.getResponseHeaders().set("Content-Type", ctype);
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.sendResponseHeaders(code, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private static String param(HttpExchange ex, String key, String def) {
        URI uri = ex.getRequestURI();
        String q = uri.getRawQuery();
        if (q == null) return def;
        for (String pair : q.split("&")) {
            int i = pair.indexOf('=');
            if (i > 0) {
                String k = pair.substring(0, i);
                if (k.equals(key)) {
                    return java.net.URLDecoder.decode(pair.substring(i + 1), StandardCharsets.UTF_8);
                }
            }
        }
        return def;
    }

    private static int paramInt(HttpExchange ex, String key, int def) {
        try {
            return Integer.parseInt(param(ex, key, String.valueOf(def)));
        } catch (NumberFormatException e) {
            return def;
        }
    }

    private static void kv(StringBuilder sb, String k, String v, boolean comma) {
        sb.append('"').append(k).append("\":\"").append(esc(v)).append('"');
        if (comma) sb.append(',');
    }

    private static void kvBool(StringBuilder sb, String k, boolean v) {
        sb.append('"').append(k).append("\":").append(v).append(',');
    }

    private static void kvBoolLast(StringBuilder sb, String k, boolean v) {
        sb.append('"').append(k).append("\":").append(v);
    }

    private static void kvNum(StringBuilder sb, String k, long v) {
        sb.append('"').append(k).append("\":").append(v).append(',');
    }

    private static void kvNumLast(StringBuilder sb, String k, long v) {
        sb.append('"').append(k).append("\":").append(v);
    }

    private static void kvF(StringBuilder sb, String k, double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) v = 0d;
        sb.append('"').append(k).append("\":").append(v).append(',');
    }

    private static String localIp() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            return "localhost";
        }
    }

    private static String esc(String s) {
        if (s == null) return "";
        StringBuilder b = new StringBuilder();
        for (char c : s.toCharArray()) {
            switch (c) {
                case '"': b.append("\\\""); break;
                case '\\': b.append("\\\\"); break;
                case '\n': b.append("\\n"); break;
                case '\r': b.append("\\r"); break;
                case '\t': b.append("\\t"); break;
                default: b.append(c);
            }
        }
        return b.toString();
    }
}
