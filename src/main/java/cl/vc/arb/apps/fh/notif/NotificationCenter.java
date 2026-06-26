package cl.vc.arb.apps.fh.notif;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * Punto unico de notificaciones del ORB. Mantiene un historial en memoria (newest-first, acotado),
 * lo persiste en Redis de forma asincrona y avisa a los listeners (p.ej. la UI para mostrar toasts).
 *
 * <p>Es UI-agnostico: cualquier parte del nucleo llama a {@link #publish}; el front se engancha con
 * {@link #addListener}. Si Redis no esta configurado/disponible, opera solo en memoria.</p>
 */
@Slf4j
public final class NotificationCenter {

    private static final NotificationCenter INSTANCE = new NotificationCenter();

    public static NotificationCenter get() {
        return INSTANCE;
    }

    private NotificationCenter() {
    }

    private final ConcurrentLinkedDeque<Notification> mem = new ConcurrentLinkedDeque<>();
    private final List<Consumer<Notification>> listeners = new CopyOnWriteArrayList<>();
    private final ExecutorService io = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "notif-io");
        t.setDaemon(true);
        return t;
    });

    private volatile RedisNotificationStore store;
    private int max = 500;

    /** Inicializa Redis (si redis.enabled=true) y precarga el historial en memoria. */
    public synchronized void init(Properties p) {
        this.max = parseInt(p.getProperty("redis.notifications.max", "500"), 500);

        boolean enabled = Boolean.parseBoolean(p.getProperty("redis.enabled", "false"));
        if (!enabled) {
            log.info("notification center init redis=off (solo memoria)");
            return;
        }

        String host = p.getProperty("redis.host", "127.0.0.1");
        int port = parseInt(p.getProperty("redis.port", "6379"), 6379);
        int timeout = parseInt(p.getProperty("redis.timeout.ms", "3000"), 3000);
        String key = p.getProperty("redis.key", "orb:notifications");

        // Password: propiedad o, si esta vacia, variable de entorno REDIS_PASSWORD (evita secreto en repo).
        String pw = p.getProperty("redis.password", "");
        if (pw == null || pw.isBlank()) {
            String env = System.getenv("REDIS_PASSWORD");
            if (env != null && !env.isBlank()) {
                pw = env;
            }
        }

        store = new RedisNotificationStore(host, port, pw, key, max, timeout);
        for (Notification n : store.loadRecent(max)) {
            mem.addLast(n); // loadRecent ya viene newest-first
        }
        log.info("notification center init redis=on cargadas={}", mem.size());
    }

    public void addListener(Consumer<Notification> l) {
        listeners.add(l);
    }

    /** Crea, almacena (memoria + Redis async) y propaga una notificacion a los listeners. */
    public Notification publish(NotificationType type, String title, String message) {
        Notification n = new Notification(
                UUID.randomUUID().toString(), type, title, message, System.currentTimeMillis());

        mem.addFirst(n);
        while (mem.size() > max) {
            mem.pollLast();
        }

        RedisNotificationStore s = store;
        if (s != null) {
            io.submit(() -> s.save(n));
        }

        for (Consumer<Notification> l : listeners) {
            try {
                l.accept(n);
            } catch (Exception e) {
                log.debug("notif listener error: {}", e.getMessage());
            }
        }

        log.info("notif {} [{}] {}", type, title, message);
        return n;
    }

    /** Snapshot del historial en memoria (newest-first). */
    public List<Notification> snapshot() {
        return new ArrayList<>(mem);
    }

    public boolean redisHealthy() {
        return store != null && store.isHealthy();
    }

    private static int parseInt(String s, int def) {
        try {
            return Integer.parseInt(s.trim());
        } catch (Exception e) {
            return def;
        }
    }
}
