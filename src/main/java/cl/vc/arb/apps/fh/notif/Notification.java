package cl.vc.arb.apps.fh.notif;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

/**
 * Notificacion del ORB (conexion, desconexion, stop, actualizacion...). Inmutable.
 * Se serializa a JSON para persistir en Redis (una entrada por notificacion).
 */
public final class Notification {

    public final String id;
    public final NotificationType type;
    public final String title;
    public final String message;
    public final long ts;

    public Notification(String id, NotificationType type, String title, String message, long ts) {
        this.id = id;
        this.type = type;
        this.title = title;
        this.message = message;
        this.ts = ts;
    }

    @SuppressWarnings("unchecked")
    public String toJson() {
        JSONObject o = new JSONObject();
        o.put("id", id);
        o.put("type", type.name());
        o.put("title", title);
        o.put("message", message);
        o.put("ts", ts);
        return o.toJSONString();
    }

    public static Notification fromJson(String s) throws Exception {
        JSONObject o = (JSONObject) new JSONParser().parse(s);
        NotificationType t;
        try {
            t = NotificationType.valueOf(String.valueOf(o.get("type")));
        } catch (Exception e) {
            t = NotificationType.INFO;
        }
        long ts = (o.get("ts") instanceof Number) ? ((Number) o.get("ts")).longValue() : 0L;
        return new Notification(str(o.get("id")), t, str(o.get("title")), str(o.get("message")), ts);
    }

    private static String str(Object o) {
        return o == null ? "" : o.toString();
    }
}
