package cl.vc.arb.apps.fh.update;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Auto-update: consulta {@code <update.url>/latest.txt}, y si hay una version mayor, descarga el
 * {@code .msi} y lanza un actualizador elevado que mata el proceso, instala en silencio y reabre.
 * La version actual la inyecta el instalador via {@code -Dapp.version} (ver build-installer.bat).
 */
@Slf4j
public final class Updater {

    private Updater() {
    }

    public static String current() {
        return System.getProperty("app.version", "0");
    }

    /** Devuelve la version nueva si en el servidor hay una mayor a la actual; null si no. */
    public static String checkLatest(String baseUrl) {
        try {
            String latest = httpGet(strip(baseUrl) + "/latest.txt").trim();
            if (!latest.isEmpty() && isNewer(latest, current())) {
                return latest;
            }
        } catch (Exception e) {
            log.debug("update check fallo: {}", e.getMessage());
        }
        return null;
    }

    public static boolean isNewer(String a, String b) {
        try {
            String[] x = a.split("\\."), y = b.split("\\.");
            int n = Math.max(x.length, y.length);
            for (int i = 0; i < n; i++) {
                int xi = i < x.length ? num(x[i]) : 0;
                int yi = i < y.length ? num(y[i]) : 0;
                if (xi != yi) return xi > yi;
            }
        } catch (Exception ignore) {
        }
        return false;
    }

    public static Path download(String baseUrl, String version) throws IOException {
        String url = strip(baseUrl) + "/ORB-BLOOMBERG-" + version + ".msi";
        Path out = Paths.get(System.getProperty("java.io.tmpdir"), "ORB-BLOOMBERG-" + version + ".msi");
        HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
        c.setConnectTimeout(15000);
        c.setReadTimeout(120000);
        try (InputStream in = c.getInputStream(); OutputStream os = Files.newOutputStream(out)) {
            in.transferTo(os);
        }
        log.info("update: descargado {} ({} bytes)", out, Files.size(out));
        return out;
    }

    /** Escribe un .bat actualizador, lo lanza ELEVADO (UAC) y sale para liberar los archivos. */
    public static void applyAndExit(Path msi) throws IOException {
        String appPath = System.getProperty("jpackage.app-path",
                "C:\\Program Files\\ORB-BLOOMBERG\\ORB-BLOOMBERG.exe");
        Path bat = Paths.get(System.getProperty("java.io.tmpdir"), "orb-update.bat");
        String content = "@echo off\r\n"
                + "timeout /t 2 >nul\r\n"
                + "taskkill /im ORB-BLOOMBERG.exe /f >nul 2>&1\r\n"
                + "msiexec /i \"" + msi + "\" /qn REBOOT=ReallySuppress\r\n"
                + "start \"\" \"" + appPath + "\"\r\n"
                + "del \"%~f0\"\r\n";
        Files.writeString(bat, content, StandardCharsets.UTF_8);
        new ProcessBuilder("powershell", "-WindowStyle", "Hidden", "-Command",
                "Start-Process -FilePath '" + bat + "' -Verb RunAs").start();
        log.info("update: lanzando actualizador elevado y saliendo…");
        System.exit(0);
    }

    private static String httpGet(String url) throws IOException {
        HttpURLConnection c = (HttpURLConnection) URI.create(url).toURL().openConnection();
        c.setConnectTimeout(8000);
        c.setReadTimeout(8000);
        try (InputStream in = c.getInputStream()) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static int num(String s) {
        String d = s.replaceAll("\\D", "");
        return d.isEmpty() ? 0 : Integer.parseInt(d);
    }

    private static String strip(String u) {
        return u.replaceAll("/+$", "");
    }
}
