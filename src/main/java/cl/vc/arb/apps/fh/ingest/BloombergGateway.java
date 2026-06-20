package cl.vc.arb.apps.fh.ingest;

import akka.actor.ActorRef;

import java.util.List;

/**
 * Contrato de la pasarela de ingesta Bloomberg, visible para el nucleo (sin dependencia de blpapi).
 *
 * <p>La implementacion real ({@code cl.vc.arb.apps.fh.bbg.BloombergSession}) vive en
 * {@code src/bloomberg/java} y solo se compila con el perfil Maven {@code -Pbloomberg} (necesita
 * el blpapi.jar). El nucleo la carga por reflexion en {@code SellSideManager}, de modo que el
 * build por defecto (simulador) compila y corre sin blpapi.</p>
 */
public interface BloombergGateway {

    /** Abre la sesion BLPAPI ({@code //blp/mktdata}, Desktop API) y deja el handler de eventos activo. */
    void start();

    /**
     * Asegura UNA suscripcion BLPAPI por {@code bbgSecurity} (dedup) y engancha el actor de topic
     * al fan-out. Cada tick entrante se entrega al actor como {@link BbgTick}.
     *
     * @param topic       topic interno del ORB (TopicGenerator)
     * @param bbgSecurity ticker Bloomberg, p.ej. "IBM US Equity", "EUR Curncy"
     * @param fields      campos a suscribir, p.ej. ["BID","ASK","LAST_PRICE",...]
     * @param topicActor  actor InversorBloombergToProto que recibe los ticks
     */
    void subscribe(String topic, String bbgSecurity, List<String> fields, ActorRef topicActor);

    /** Desengancha el actor; si la security se queda sin actores, retira la suscripcion BLPAPI. */
    void unsubscribe(String topic, ActorRef topicActor);

    /** true si la sesion BLPAPI esta abierta y operativa. */
    default boolean isConnected() {
        return false;
    }

    /** Cierra la sesion BLPAPI. */
    default void stop() {
    }
}
