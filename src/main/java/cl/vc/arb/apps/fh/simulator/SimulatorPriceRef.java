package cl.vc.arb.apps.fh.simulator;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Reference price entry loaded from simulator-prices.json.
 * Used by SimulatorEngine to generate market data within allowed limits.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SimulatorPriceRef {

    /** Ticker symbol, e.g. "AGUAS-A" */
    private String symbol;

    /** Last traded price — primary reference for ±0.5% band */
    private double last;

    /** Opening price of the session */
    private double open;

    /** Daily variation percentage, e.g. -1.23 */
    private double variation;

    /** Reference bid quantity (used to size simulated quantities) */
    private double bidQty;

    /** Reference ask quantity (used to size simulated quantities) */
    private double askQty;
}
