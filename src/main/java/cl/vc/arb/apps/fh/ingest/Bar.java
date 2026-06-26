package cl.vc.arb.apps.fh.ingest;

/**
 * Una vela/barra OHLCV ya extraida a tipos planos (sin dependencia de blpapi), tal como la
 * devuelve {@code BloombergSession.history(...)} a la GUI para dibujar el grafico de velas.
 *
 * @param time   epoch millis del inicio de la barra (best-effort; 0 si no se pudo resolver)
 * @param open   precio de apertura
 * @param high   maximo
 * @param low    minimo
 * @param close  cierre
 * @param volume volumen (NaN si no vino)
 */
public final class Bar {

    public final long time;
    public final double open, high, low, close, volume;

    public Bar(long time, double open, double high, double low, double close, double volume) {
        this.time = time;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }
}
