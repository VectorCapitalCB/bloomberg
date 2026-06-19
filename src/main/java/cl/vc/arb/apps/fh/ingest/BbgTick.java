package cl.vc.arb.apps.fh.ingest;

import java.util.Map;

/**
 * Tick crudo de Bloomberg ya extraido a tipos planos (sin dependencia de blpapi), tal como lo
 * envia {@code BloombergSession} al actor de topic. El actor lo traduce a los mensajes proto
 * ({@code Statistic}/{@code DataBook}/{@code Trade}) y los publica al fan-out.
 *
 * @param security  ticker Bloomberg de origen (informativo; el actor ya conoce su topic)
 * @param nums      campos numericos presentes en este tick (BID, ASK, BID_SIZE, ASK_SIZE,
 *                  LAST_PRICE, SIZE_LAST_TRADE, VOLUME, OPEN, HIGH, LOW, PREV_SES_LAST_PRICE, ...)
 * @param eventType valor de MKTDATA_EVENT_TYPE si vino ("TRADE","BID","ASK",...); puede ser null
 */
public final class BbgTick {

    public final String security;
    public final Map<String, Double> nums;
    public final String eventType;

    public BbgTick(String security, Map<String, Double> nums, String eventType) {
        this.security = security;
        this.nums = nums;
        this.eventType = eventType;
    }
}
