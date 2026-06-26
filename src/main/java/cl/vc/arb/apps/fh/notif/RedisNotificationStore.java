package cl.vc.arb.apps.fh.notif;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Persistencia de notificaciones en Redis usando una LISTA (newest-first):
 * {@code LPUSH <key> <json>} + {@code LTRIM <key> 0 max-1} para acotar el historial.
 * Si Redis no esta disponible el ORB sigue operando solo con memoria (best-effort).
 */
@Slf4j
public class RedisNotificationStore {

    private final JedisPool pool;
    private final String key;
    private final int max;
    private volatile boolean healthy;

    public RedisNotificationStore(String host, int port, String password, String key, int max, int timeoutMs) {
        this.key = key;
        this.max = max;

        JedisPoolConfig cfg = new JedisPoolConfig();
        cfg.setMaxTotal(8);
        cfg.setMaxIdle(4);
        cfg.setMinIdle(1);
        cfg.setTestOnBorrow(true);

        String pw = (password == null || password.isBlank()) ? null : password;
        this.pool = new JedisPool(cfg, host, port, timeoutMs, pw);

        try (Jedis j = pool.getResource()) {
            j.ping();
            healthy = true;
            log.info("redis notif store OK {}:{} key={} max={}", host, port, key, max);
        } catch (Exception e) {
            healthy = false;
            log.warn("redis notif store NO disponible {}:{} ({}) -> solo memoria", host, port, e.getMessage());
        }
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void save(Notification n) {
        try (Jedis j = pool.getResource()) {
            j.lpush(key, n.toJson());
            j.ltrim(key, 0, max - 1);
            healthy = true;
        } catch (Exception e) {
            healthy = false;
            log.warn("redis notif save fallo: {}", e.getMessage());
        }
    }

    /** Devuelve hasta {@code limit} notificaciones, las mas recientes primero. */
    public List<Notification> loadRecent(int limit) {
        List<Notification> out = new ArrayList<>();
        try (Jedis j = pool.getResource()) {
            List<String> raw = j.lrange(key, 0, limit - 1);
            for (String s : raw) {
                try {
                    out.add(Notification.fromJson(s));
                } catch (Exception ex) {
                    log.debug("notif parse fallo: {}", ex.getMessage());
                }
            }
            healthy = true;
        } catch (Exception e) {
            healthy = false;
            log.warn("redis notif load fallo: {}", e.getMessage());
        }
        return out;
    }

    public void close() {
        try {
            pool.close();
        } catch (Exception ignore) {
            // best-effort
        }
    }
}
