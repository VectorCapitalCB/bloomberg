package cl.vc.arb.apps.fh.utils;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class BookSnapshot {

    private final String id;
    private final String symbol;
    private final RoutingMessage.SettlType settlType;
    private Boolean receivedSnapshot = false;
    private final MarketDataMessage.SecurityExchangeMarketData securityExchangeMarketData;
    private List<MarketDataMessage.DataBook> bid = new ArrayList<>();
    private List<MarketDataMessage.DataBook> ask = new ArrayList<>();
    private List<MarketDataMessage.Trade> trades = new ArrayList<>();


    private MarketDataMessage.Statistic.Builder statistic = MarketDataMessage.Statistic.newBuilder();
    private MarketDataMessage.Ohlcv.Builder ohlcv = MarketDataMessage.Ohlcv.newBuilder();

    public BookSnapshot(String id, MarketDataMessage.Subscribe subscribe) {
        this.id = id;
        this.symbol = subscribe.getSymbol();
        this.securityExchangeMarketData = subscribe.getSecurityExchange();
        this.settlType = subscribe.getSettlType();
        statistic.setSymbol(symbol);
        statistic.setSecurityExchange(securityExchangeMarketData);
        statistic.setSettlType(settlType);
    }
}
