package cl.vc.arb.apps.fh.utils;

import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import com.google.protobuf.util.Timestamps;
import lombok.extern.slf4j.Slf4j;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Random;

@Slf4j
public class Helper {

    public static final String LOGON_FIX     = "/LOGON_FIX/";

    /** Event-bus topic for market-wide trade broadcasts (global trade feed). */
    public static final String TRADE_GENERAL = "/TRADE_GENERAL/";

    public static String convertSettlType(RoutingMessage.SettlType settlType) {
        if (settlType.equals(RoutingMessage.SettlType.REGULAR)
                || settlType.equals(RoutingMessage.SettlType.T2)) {
            return "|||";
        } else if (settlType.equals(RoutingMessage.SettlType.CASH)) {
            return "PH|||";
        } else if (settlType.equals(RoutingMessage.SettlType.NEXT_DAY)) {
            return "PM|||";
        } else if (settlType.equals(RoutingMessage.SettlType.T3)) {
            return "T+3|||";
        } else if (settlType.equals(RoutingMessage.SettlType.T5)) {
            return "T+5|||";
        } else {
            return settlType.name();
        }
    }

    /**
     * Clave canonica del instrumento EN LA BOLSA = symbol + settl(466) + exchange [+ currency].
     * No incluye el securityType del cliente: asi CFI y CS del mismo papel producen la MISMA clave
     * -> una sola suscripcion al proveedor.
     */
    public static String exchangeKey(String symbol, RoutingMessage.SettlType settlType, String exchangeName, String currency) {
        StringBuilder sb = new StringBuilder(symbol).append('|')
                .append(convertSettlType(settlType)).append('|')
                .append(exchangeName);
        if (currency != null && !currency.isBlank()) {
            sb.append('|').append(currency);
        }
        return sb.toString();
    }


    public static double generatePrice(String symbol) {
        try {
            double price = 0d;
            if ("AGUAS-A".equals(symbol)) {
                price = generateRandomDouble(22892, 22892);
            } else if ("ANDINA-B".equals(symbol)) {
                price = generateRandomDouble(2010, 2005);
            } else if ("BCI".equals(symbol) || "BCI-OSA".equals(symbol)) {
                price = generateRandomDouble(25600, 26001);
            } else if ("USD/CLP".equals(symbol)) {
                price = generateRandomDouble(950, 980);
            } else if ("SQM-B".equals(symbol)) {
                price = generateRandomDouble(37600, 37900);
            } else if ("SQM".equals(symbol)) {
                price = generateRandomDouble(58.38, 58.70);
            } else {
                price = generateRandomDouble(0.0, 100.0);
            }
            return price;
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }

        return 0d;
    }


    public static BookSnapshot generateRandomSnapshot(BookSnapshot bookSnapshot) {


        List<MarketDataMessage.DataBook> dataBooksBid = generateRandomDataBooks(MarketDataMessage.TypeBook.BID, bookSnapshot.getSymbol());
        List<MarketDataMessage.DataBook> dataBooksAsk = generateRandomDataBooks(MarketDataMessage.TypeBook.ASK, bookSnapshot.getSymbol());

        MarketDataMessage.Statistic.Builder statistic = generateRandomStatistic(bookSnapshot);
        MarketDataMessage.Ohlcv ohlcv = generateRandomOhlcv(bookSnapshot);
        statistic.setOhlcv(ohlcv);
        statistic.setClose(ohlcv.getClose());
        statistic.setOpen(ohlcv.getOpen());
        statistic.setLow(ohlcv.getLow());
        statistic.setHigh(ohlcv.getHigh());
        statistic.setVolume(ohlcv.getVolume());


        // que siempre la venta sea mayor que la compra
        if (dataBooksBid.get(0).getPrice() < dataBooksAsk.get(0).getSize()) {
            statistic.setBidPx(dataBooksBid.getFirst().getPrice());
            statistic.setBidQty(dataBooksBid.getFirst().getSize());
            statistic.setAskPx(dataBooksAsk.get(0).getPrice());
            statistic.setAskQty(dataBooksAsk.get(0).getSize());

            List<MarketDataMessage.Trade> trades = generateRandomTrades(bookSnapshot);
            bookSnapshot.setAsk(dataBooksAsk);
            bookSnapshot.setBid(dataBooksBid);
            bookSnapshot.getTrades().add(trades.get(0));
            bookSnapshot.setStatistic(statistic);

        } else {

            statistic.setAskPx(dataBooksBid.get(0).getPrice());
            statistic.setAskQty(dataBooksBid.get(0).getSize());
            statistic.setBidPx(dataBooksAsk.get(0).getPrice());
            statistic.setBidQty(dataBooksAsk.get(0).getSize());

            List<MarketDataMessage.Trade> trades = generateRandomTrades(bookSnapshot);
            bookSnapshot.setAsk(dataBooksBid);
            bookSnapshot.setBid(dataBooksAsk);
            bookSnapshot.getTrades().add(trades.get(0));
            bookSnapshot.setStatistic(statistic);


        }
        return bookSnapshot;

    }


    public static List<MarketDataMessage.DataBook> generateRandomDataBooks(MarketDataMessage.TypeBook typeBook, String symbol) {
        List<MarketDataMessage.DataBook> dataBooks = new ArrayList<>();
        int numBooks = new Random().nextInt(800) + 1;

        for (int i = 0; i < numBooks; i++) {

            double price = generatePrice(symbol);
            double size = new Random().nextDouble();

            MarketDataMessage.DataBook dataBook = MarketDataMessage.DataBook.newBuilder()
                    .setPrice(price)
                    .setSize(size)
                    .setTypeBook(typeBook)
                    .build();

            dataBooks.add(dataBook);
        }
        return dataBooks;
    }

    public static MarketDataMessage.Statistic.Builder generateRandomStatistic(BookSnapshot bookSnapshot) {
        double amount = generateRandomDouble(0.0, 1000.0);
        double vwap = generateRandomDouble(0.0, 100.0);
        double imbalance = generateRandomDouble(0.0, 1.0);
        String ratio = generateRandomString();
        String id = generateRandomString();
        double tradeVolume = generateRandomDouble(0.0, 1000.0);
        double delta = generateRandomDouble(-1.0, 1.0);
        double previousClose = generatePrice(bookSnapshot.getSymbol());
        double referentialPrice = generatePrice(bookSnapshot.getSymbol());
        double indicativeOpening = generateRandomDouble(0.0, 100.0);
        double lastPx = generatePrice(bookSnapshot.getSymbol());
        double tickDirection = generateRandomDouble(-1.0, 1.0);
        double priceT = generateRandomDouble(0.0, 100.0);
        double amountT = generateRandomDouble(0.0, 1000.0);
        double desT = generateRandomDouble(0.0, 100.0);

        MarketDataMessage.Statistic.Builder statistic = MarketDataMessage.Statistic.newBuilder()
                .setAmount(amount)
                .setVwap(vwap)
                .setImbalance(imbalance)
                .setSymbol(bookSnapshot.getSymbol())
                .setRatio(ratio)
                .setId(id)
                .setOwnDemand(new Random().nextDouble())
                .setTotalDemand(new Random().nextDouble())
                .setAmountTheoric(new Random().nextDouble())
                .setPriceTheoric(new Random().nextDouble())
                .setDesbalTheoric(desT)
                .setAmountTheoric(amountT)
                .setPriceTheoric(priceT)
                .setTradeVolume(tradeVolume)
                .setDelta(delta)
                .setPreviusClose(previousClose)
                .setIndicativeOpening(indicativeOpening)
                .setLast(lastPx)
                .setSymbol(bookSnapshot.getSymbol())
                .setSettlType(bookSnapshot.getSettlType())
                .setReferencialPrice(referentialPrice)
                .setTickDirecion(tickDirection)
                .setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData())
                .setId(bookSnapshot.getId());

        return statistic;
    }


    public static double generateRandomDouble(double min, double max) {
        double randomValue = min + (max - min) * new Random().nextDouble();
        // Crea un formato con punto como separador decimal
        DecimalFormatSymbols symbols = new DecimalFormatSymbols(Locale.US);
        symbols.setDecimalSeparator('.');
        DecimalFormat decimalFormat = new DecimalFormat("#.00", symbols);
        // Formatea el número con punto como separador decimal
        return Double.valueOf(decimalFormat.format(randomValue));
    }


    public static MarketDataMessage.Ohlcv generateRandomOhlcv(BookSnapshot bookSnapshot) {
        double close = generatePrice(bookSnapshot.getSymbol());
        double open = generatePrice(bookSnapshot.getSymbol());
        double low = generatePrice(bookSnapshot.getSymbol());
        double high = generatePrice(bookSnapshot.getSymbol());
        double volume = generatePrice(bookSnapshot.getSymbol());

        MarketDataMessage.Ohlcv ohlcv = MarketDataMessage.Ohlcv.newBuilder()
                .setClose(close)
                .setOpen(open)
                .setLow(low)
                .setHigh(high)
                .setVolume(volume)
                .setId(bookSnapshot.getId())
                .setSymbol(bookSnapshot.getSymbol())
                .setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData())
                .setSettlType(bookSnapshot.getSettlType())
                .build();

        return ohlcv;
    }

    public static List<MarketDataMessage.Trade> generateRandomTrades(BookSnapshot bookSnapshot) {
        List<MarketDataMessage.Trade> trades = new ArrayList<>();
        int numTrades = new Random().nextInt(1) + 1;
        for (int i = 0; i < numTrades; i++) {
            double price = generatePrice(bookSnapshot.getSymbol());
            double qty = new Random().nextDouble();
            double amount = new Random().nextDouble();
            String buyer = "Buyer" + (i + 1);
            String seller = "Seller" + (i + 1);
            MarketDataMessage.Trade.Builder trade = MarketDataMessage.Trade.newBuilder();
            trade.setT(Timestamps.fromMillis(System.currentTimeMillis()));
            trade.setBuyer(buyer);
            trade.setPrice(price);
            trade.setSeller(seller);
            trade.setQty(qty);
            trade.setAmount(amount);
            trade.setId(bookSnapshot.getId());
            trade.setSymbol(bookSnapshot.getSymbol());
            trade.setSecurityExchange(bookSnapshot.getSecurityExchangeMarketData());
            trade.setSettlType(bookSnapshot.getSettlType());
            trades.add(trade.build());
        }
        return trades;
    }


    private static String generateRandomString() {
        int length = 10;
        String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(chars.length());
            sb.append(chars.charAt(index));
        }
        return sb.toString();
    }


    public static String channels(MarketDataMessage.Subscribe subscribe) {
        String[] chs = {
                "book%40" + subscribe.getSymbol().replace("/", "").toLowerCase(),
                "trades%40" + subscribe.getSymbol().replace("/", "").toLowerCase(),
        };
        return String.join(",", chs);
    }


}
