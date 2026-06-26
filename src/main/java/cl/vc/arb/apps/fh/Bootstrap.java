package cl.vc.arb.apps.fh;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Punto de entrada del SERVICIO de Windows (lo usa el instalador jpackage como --main-class).
 *
 * <p>Antes de arrancar la app, deja la configuracion y los logs FUERA de la carpeta de instalacion
 * (en {@code %PROGRAMDATA%\ORB-BLOOMBERG\}), de modo que una reinstalacion/actualizacion NO pise el
 * config editado ni los logs. Siembra un {@code BLOOMBERG.properties} por defecto la primera vez.</p>
 *
 * <p>No usa logback (se inicializa despues, una vez fijado {@code log.dir}). Para correr en dev con
 * logs locales: pasar {@code -Dlog.dir=logs} y/o el archivo de propiedades como argumento.</p>
 */
public final class Bootstrap {

    private static final String APP_DIR_NAME = "ORB-BLOOMBERG";
    private static final String DEFAULT_CONFIG = "BLOOMBERG.properties";

    /** Mantiene el lock de instancia unica abierto toda la vida del proceso. */
    @SuppressWarnings("unused")
    private static java.io.RandomAccessFile INSTANCE_LOCK;

    public static void main(String[] args) {
        try {
            String base = System.getenv("PROGRAMDATA");
            if (base == null || base.isBlank()) {
                base = System.getProperty("user.dir");
            }
            Path home = Paths.get(base, APP_DIR_NAME);
            Path configDir = home.resolve("config");
            Path logDir = home.resolve("logs");
            Files.createDirectories(configDir);
            Files.createDirectories(logDir);

            // logback lee ${log.dir}; respeta el valor si ya vino por -Dlog.dir
            if (System.getProperty("log.dir") == null) {
                System.setProperty("log.dir", logDir.toString());
            }

            // Instancia unica: si ya hay una corriendo, abre el panel existente y sale (no duplica).
            java.io.RandomAccessFile lockRaf = new java.io.RandomAccessFile(home.resolve("orb.lock").toFile(), "rw");
            if (lockRaf.getChannel().tryLock() == null) {
                System.out.println("[Bootstrap] ORB-BLOOMBERG ya esta corriendo; abro el panel y salgo.");
                openBrowser("http://localhost:8090/");
                System.exit(0);
            }
            INSTANCE_LOCK = lockRaf;

            // Resolver el archivo de propiedades: argumento explicito, o el de ProgramData (sembrado).
            String configPath;
            if (args.length > 0 && args[0] != null && !args[0].isBlank()) {
                configPath = args[0];
            } else {
                Path def = configDir.resolve(DEFAULT_CONFIG);
                if (!Files.exists(def)) {
                    seedDefaultConfig(def, logDir);
                    System.out.println("[Bootstrap] config por defecto creado en " + def);
                }
                configPath = def.toString();
            }

            System.out.println("[Bootstrap] home=" + home + " config=" + configPath + " logs=" + logDir);
            MainApp.main(new String[]{configPath});

        } catch (Throwable t) {
            System.err.println("[Bootstrap] error fatal: " + t.getMessage());
            t.printStackTrace();
            System.exit(1);
        }
    }

    private static void openBrowser(String url) {
        try {
            if (!java.awt.GraphicsEnvironment.isHeadless() && java.awt.Desktop.isDesktopSupported()
                    && java.awt.Desktop.getDesktop().isSupported(java.awt.Desktop.Action.BROWSE)) {
                java.awt.Desktop.getDesktop().browse(new java.net.URI(url));
            }
        } catch (Throwable ignore) {
        }
    }

    /** Escribe el config embebido y fuerza path.logs al dir persistente (ultima clave gana). */
    private static void seedDefaultConfig(Path target, Path logDir) throws Exception {
        StringBuilder sb = new StringBuilder();
        try (InputStream in = Bootstrap.class.getResourceAsStream("/" + DEFAULT_CONFIG)) {
            if (in != null) {
                sb.append(new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
        }
        sb.append(System.lineSeparator())
          .append("# path.logs forzado por el instalador (persistente entre actualizaciones)")
          .append(System.lineSeparator())
          .append("path.logs=").append(logDir.toString().replace('\\', '/')).append('/')
          .append(System.lineSeparator());
        Files.writeString(target, sb.toString(), StandardCharsets.UTF_8);
    }
}
