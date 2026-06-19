package cl.vc.arb.apps.fh.simulator;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.utils.Helper;
import cl.vc.module.protocolbuff.akka.Envelope;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Akka actor that periodically broadcasts {@link MarketDataMessage.Trade}
 * messages for random symbols to the {@link Helper#TRADE_GENERAL} event-bus
 * topic.  All connected {@code SessionTracker} actors subscribe to this topic
 * and forward the trades to their clients, simulating a live market-wide
 * trade feed.
 */
@Slf4j
public class GlobalTradeSimulator extends AbstractActor {

    private final Map<String, SimulatorPriceRef> prices;
    private final int refreshMs;
    private final MarketDataMessage.SecurityExchangeMarketData exchange;
    private final List<String> symbols;
    private final Random random = new Random();

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "global-trade-sim");
        t.setDaemon(true);
        return t;
    });

    public static Props props(Map<String, SimulatorPriceRef> prices,
                              int refreshMs,
                              MarketDataMessage.SecurityExchangeMarketData exchange) {
        return Props.create(GlobalTradeSimulator.class, prices, refreshMs, exchange);
    }

    public GlobalTradeSimulator(Map<String, SimulatorPriceRef> prices,
                                int refreshMs,
                                MarketDataMessage.SecurityExchangeMarketData exchange) {
        this.prices    = prices;
        this.refreshMs = refreshMs;
        this.exchange  = exchange;
        this.symbols   = new ArrayList<>(prices.keySet());
    }

    @Override
    public void preStart() {
        scheduler.scheduleAtFixedRate(
                this::publishRandomTrades,
                refreshMs,       // initial delay — let per-symbol simulators start first
                refreshMs,
                TimeUnit.MILLISECONDS);
        log.info("global trade simulator started symbols={} refreshMs={}", symbols.size(), refreshMs);
    }

    @Override
    public void postStop() {
        scheduler.shutdownNow();
        log.info("global trade simulator stopped");
    }

    private void publishRandomTrades() {
        try {
            if (symbols.isEmpty()) return;
            // Publish 1-3 random symbol trades per tick
            int count = 1 + random.nextInt(Math.min(3, symbols.size()));
            for (int i = 0; i < count; i++) {
                String symbol = symbols.get(random.nextInt(symbols.size()));
                SimulatorPriceRef ref = prices.get(symbol);
                if (ref == null) continue;

                MarketDataMessage.Trade trade = SimulatorEngine.generateGlobalTrade(ref, exchange);
                if (trade != null) {
                    MainApp.getEventBus().publish(new Envelope(Helper.TRADE_GENERAL, trade));
                }
            }
        } catch (Exception e) {
            log.error("global trade simulator error", e);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().build();
    }
}
