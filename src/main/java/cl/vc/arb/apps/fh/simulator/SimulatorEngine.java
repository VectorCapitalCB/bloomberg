package cl.vc.arb.apps.fh.simulator;

import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.generator.TimeGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Core market data simulator engine.
 *
 * Rules enforced on every call to {@link #populate}:
 * <ul>
 *   <li>All prices within ±0.5 % of the reference (last) price.</li>
 *   <li>Best bid strictly less than best ask (no crossed book).</li>
 *   <li>Bid-ask spread ≤ 10 basis points of the mid price.</li>
 *   <li>Order book depth: at most {@value #MAX_BOOK_LEVELS} levels per side.</li>
 * </ul>
 */
@Slf4j
public class SimulatorEngine {

    private static final int MAX_BOOK_LEVELS = 5;

    /** Maximum price drift from reference: ±0.5 % */
    private static final double MAX_DRIFT_PCT = 0.005;

    /** Maximum bid-ask spread: 10 basis points = 0.10 % */
    private static final double MAX_SPREAD_BPS = 0.0010;

    /** Maximum trade history kept in the snapshot */
    private static final int MAX_TRADE_HISTORY = 30;

    private static final Random RANDOM = new Random();

    // ------------------------------------------------------------------ //
    //  Public API                                                          //
    // ------------------------------------------------------------------ //

    /**
     * Populates (or updates) a {@link BookSnapshot} with freshly simulated
     * market data based on the given reference price.
     *
     * @param snapshot live snapshot object to mutate
     * @param ref      reference data loaded from simulator-prices.json
     */
    public static void populate(BookSnapshot snapshot, SimulatorPriceRef ref) {
        double refPx = ref.getLast();
        if (refPx <= 0) {
            log.warn("simulator skip symbol={} refPx={}", ref.getSymbol(), refPx);
            return;
        }

        double tick     = getMinTick(refPx);
        double bandLow  = refPx * (1.0 - MAX_DRIFT_PCT);
        double bandHigh = refPx * (1.0 + MAX_DRIFT_PCT);

        // ---- spread (1..5 bps of refPx, total ≤ 10 bps) ----- //
        double maxHalfSpread = refPx * MAX_SPREAD_BPS / 2.0;
        double halfSpread    = maxHalfSpread * (0.2 + RANDOM.nextDouble() * 0.8);
        halfSpread = Math.max(halfSpread, tick);

        // ---- mid price drifts within band (leaving room for half-spread) //
        double midMin = bandLow  + halfSpread;
        double midMax = bandHigh - halfSpread;
        if (midMin > midMax) { midMin = midMax = refPx; }
        double mid = midMin + RANDOM.nextDouble() * (midMax - midMin);

        double bestBid = roundToTick(mid - halfSpread, tick);
        double bestAsk = roundToTick(mid + halfSpread, tick);
        if (bestBid >= bestAsk) bestAsk = roundToTick(bestBid + tick, tick);

        // ---- deeper book levels (stay inside the ±0.5 % band) ---------- //
        double bidRoom  = Math.max(bestBid - bandLow,   tick);
        double askRoom  = Math.max(bandHigh - bestAsk,  tick);
        double bidStep  = Math.max(roundToTick(bidRoom / (MAX_BOOK_LEVELS - 1), tick), tick);
        double askStep  = Math.max(roundToTick(askRoom / (MAX_BOOK_LEVELS - 1), tick), tick);

        List<MarketDataMessage.DataBook> bids = new ArrayList<>();
        List<MarketDataMessage.DataBook> asks = new ArrayList<>();

        for (int i = 0; i < MAX_BOOK_LEVELS; i++) {
            double bidPx = Math.max(roundToTick(bestBid - i * bidStep, tick), bandLow);
            double askPx = Math.min(roundToTick(bestAsk + i * askStep, tick), bandHigh);
            if (bidPx <= 0 || askPx <= 0 || bidPx >= askPx) break;

            bids.add(MarketDataMessage.DataBook.newBuilder()
                    .setPrice(bidPx).setSize(randomQty(ref.getBidQty()))
                    .setTypeBook(MarketDataMessage.TypeBook.BID)
                    .setSymbol(snapshot.getSymbol())
                    .setSecurityExchange(snapshot.getSecurityExchangeMarketData())
                    .build());

            asks.add(MarketDataMessage.DataBook.newBuilder()
                    .setPrice(askPx).setSize(randomQty(ref.getAskQty()))
                    .setTypeBook(MarketDataMessage.TypeBook.ASK)
                    .setSymbol(snapshot.getSymbol())
                    .setSecurityExchange(snapshot.getSecurityExchangeMarketData())
                    .build());
        }

        snapshot.setBid(bids);
        snapshot.setAsk(asks);

        // ---- statistic ------------------------------------------------- //
        double lastPx  = roundToTick(bandLow + RANDOM.nextDouble() * (bandHigh - bandLow), tick);
        double highPx  = roundToTick(Math.min(refPx * (1 + MAX_DRIFT_PCT * RANDOM.nextDouble()), bandHigh), tick);
        double lowPx   = roundToTick(Math.max(refPx * (1 - MAX_DRIFT_PCT * RANDOM.nextDouble()), bandLow),  tick);
        double vwap    = roundToTick(refPx * (0.9990 + RANDOM.nextDouble() * 0.002), tick);
        double volume  = Math.max(1, ref.getBidQty() + ref.getAskQty());
        double amount  = lastPx * volume;

        double prevClose = ref.getVariation() != 0
                ? roundToTick(refPx / (1.0 + ref.getVariation() / 100.0), tick)
                : refPx;

        snapshot.getStatistic()
                .setBidPx(bestBid).setBidQty(randomQty(ref.getBidQty()))
                .setAskPx(bestAsk).setAskQty(randomQty(ref.getAskQty()))
                .setLast(lastPx)
                .setPreviusClose(prevClose)
                .setOpen(ref.getOpen() > 0 ? ref.getOpen() : refPx)
                .setHigh(highPx).setLow(lowPx)
                .setClose(lastPx)
                .setVolume(volume)
                .setVwap(vwap)
                .setAmount(amount)
                .setTradeVolume(volume)
                .setDelta(ref.getVariation());

        snapshot.getOhlcv()
                .setOpen(ref.getOpen() > 0 ? ref.getOpen() : refPx)
                .setHigh(highPx).setLow(lowPx)
                .setClose(lastPx).setVolume(volume)
                .setSymbol(snapshot.getSymbol())
                .setSecurityExchange(snapshot.getSecurityExchangeMarketData())
                .setSettlType(snapshot.getSettlType());

        snapshot.getStatistic().setOhlcv(snapshot.getOhlcv());

        // ---- trade ----------------------------------------------------- //
        double tradePx  = roundToTick(bandLow + RANDOM.nextDouble() * (bandHigh - bandLow), tick);
        double tradeQty = 1000 + RANDOM.nextInt(49000);

        MarketDataMessage.Trade trade = MarketDataMessage.Trade.newBuilder()
                .setPrice(tradePx).setQty(tradeQty).setAmount(tradePx * tradeQty)
                .setT(TimeGenerator.getTimeProto())
                .setBuyer("SIM-B" + String.format("%02d", RANDOM.nextInt(100)))
                .setSeller("SIM-S" + String.format("%02d", RANDOM.nextInt(100)))
                .setId(snapshot.getId())
                .setSymbol(snapshot.getSymbol())
                .setSecurityExchange(snapshot.getSecurityExchangeMarketData())
                .setSettlType(snapshot.getSettlType())
                .setIdGenerico(IDGenerator.getID())
                .build();

        snapshot.getTrades().add(trade);
        // Trim trade history
        while (snapshot.getTrades().size() > MAX_TRADE_HISTORY) {
            snapshot.getTrades().remove(0);
        }

        snapshot.setReceivedSnapshot(true);
    }

    /**
     * Generates a standalone {@link MarketDataMessage.Trade} for a given symbol
     * to be broadcast as a general market trade (not tied to any subscription).
     *
     * @param ref      reference data for the symbol
     * @param exchange the security exchange enum value
     * @return a built Trade proto, or {@code null} if reference price is invalid
     */
    public static MarketDataMessage.Trade generateGlobalTrade(
            SimulatorPriceRef ref,
            MarketDataMessage.SecurityExchangeMarketData exchange) {

        double refPx = ref.getLast();
        if (refPx <= 0) return null;

        double tick  = getMinTick(refPx);
        double drift = refPx * (RANDOM.nextDouble() * 2.0 - 1.0) * MAX_DRIFT_PCT;
        double px    = roundToTick(Math.max(refPx * 0.995, Math.min(refPx * 1.005, refPx + drift)), tick);
        double qty   = 1000 + RANDOM.nextInt(49000);

        return MarketDataMessage.Trade.newBuilder()
                .setPrice(px).setQty(qty).setAmount(px * qty)
                .setT(TimeGenerator.getTimeProto())
                .setBuyer("SIM-B" + String.format("%02d", RANDOM.nextInt(100)))
                .setSeller("SIM-S" + String.format("%02d", RANDOM.nextInt(100)))
                .setSymbol(ref.getSymbol())
                .setSecurityExchange(exchange)
                .setIdGenerico(IDGenerator.getID())
                .build();
    }

    // ------------------------------------------------------------------ //
    //  Helpers                                                             //
    // ------------------------------------------------------------------ //

    private static double randomQty(double refQty) {
        if (refQty <= 0) refQty = 1000;
        double factor = 0.1 + RANDOM.nextDouble() * 1.9;
        return Math.max(100, Math.round(refQty * factor));
    }

    /**
     * Returns the minimum price tick for a given price magnitude,
     * approximating typical BCS market conventions.
     */
    static double getMinTick(double price) {
        if (price < 1.0)      return 0.0001;
        if (price < 10.0)     return 0.01;
        if (price < 100.0)    return 0.01;
        if (price < 1000.0)   return 0.10;
        if (price < 5000.0)   return 1.0;
        if (price < 10000.0)  return 10.0;
        return 100.0;
    }

    static double roundToTick(double price, double tick) {
        return Math.round(price / tick) * tick;
    }
}
