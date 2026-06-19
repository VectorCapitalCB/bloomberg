package cl.vc.arb.apps.fh.bs;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.ss.SellSideManager;
import cl.vc.arb.apps.fh.utils.BookSnapshot;
import cl.vc.module.protocolbuff.generator.TopicGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class TopicDistributor extends AbstractActor {
    private static final int MAX_DROPPED_UPDATES_BEFORE_EVICT = 1024;

    private final MarketDataMessage.Subscribe seedSubscribe;
    private final String topic;
    private final Map<String, SubscriberState> subscribers = new LinkedHashMap<>();

    private BookSnapshot latestSnapshot;

    public static Props props(MarketDataMessage.Subscribe subscribe) {
        return Props.create(TopicDistributor.class, subscribe);
    }

    public TopicDistributor(MarketDataMessage.Subscribe subscribe) {
        this.seedSubscribe = subscribe;
        this.topic = TopicGenerator.getTopicMKD(subscribe);
    }

    @Override
    public void preStart() {

        MainApp.getTopicDistributorMaps().put(topic, getSelf());
        MainApp.getEventBus().subscribe(getSelf(), topic);

        latestSnapshot = MainApp.bookHasmap.get(topic);
        if (latestSnapshot == null) {
            MainApp.getSellSideManager().tell(
                    new SellSideManager.Subscribe(seedSubscribe, getSelf()),
                    ActorRef.noSender());
        } else {
            getSelf().tell(latestSnapshot, ActorRef.noSender());
        }
    }

    @Override
    public void postStop() {
        MainApp.getEventBus().unsubscribe(getSelf(), topic);
        MainApp.getTopicDistributorMaps().remove(topic, getSelf());
        if (!subscribers.isEmpty()) {
            MainApp.getActiveTopicSubscribers().add(-subscribers.size());
            subscribers.clear();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AddSubscriber.class, this::onAddSubscriber)
                .match(RemoveSubscriber.class, this::onRemoveSubscriber)
                .match(BookSnapshot.class, this::onBookSnapshot)
                .match(MarketDataMessage.Trade.Builder.class, this::onTrade)
                .build();
    }

    private void onAddSubscriber(AddSubscriber msg) {
        SubscriberState state = subscribers.computeIfAbsent(
                msg.getSubscriberKey(),
                ignored -> {
                    MainApp.getActiveTopicSubscribers().increment();
                    return new SubscriberState(msg.getSubscriberKey(), msg.getCtx().channel(), msg.getSubscribe());
                });

        state.channel = msg.getCtx().channel();
        state.subscribe = msg.getSubscribe();
        state.droppedUpdates = 0;
        state.snapshotSent = false; // reset so reconnecting subscriber gets a full fresh snapshot
        state.bookSnapshotSent = false; // y un Snapshot con libro cuando aparezcan puntas

        if (latestSnapshot != null) {
            boolean initialSent = sendSnapshotIfNeeded(state, latestSnapshot);
            if (state.snapshotSent && !initialSent) {
                sendIncremental(state,
                        buildStatistic(latestSnapshot, state),
                        buildIncremental(latestSnapshot, state));
            }
            flushChannels();
        }
    }

    private void onRemoveSubscriber(RemoveSubscriber msg) {
        if (subscribers.remove(msg.getSubscriberKey()) != null) {
            MainApp.getActiveTopicSubscribers().decrement();
        }
        stopIfEmpty();
    }

    private void onBookSnapshot(BookSnapshot snapshot) {
        latestSnapshot = snapshot;
        Map<SubscriptionProfile, List<SubscriberState>> groupedSubscribers = groupByProfile();

        for (Map.Entry<SubscriptionProfile, List<SubscriberState>> entry : groupedSubscribers.entrySet()) {
            SubscriptionProfile profile = entry.getKey();
            for (SubscriberState state : entry.getValue()) {
                boolean initialSent = sendSnapshotIfNeeded(state, snapshot);
                if (state.snapshotSent && !initialSent) {
                    sendIncremental(state,
                            buildStatistic(snapshot, state, profile),
                            buildIncremental(snapshot, state, profile));
                }
            }
        }
        flushChannels();
    }

    private void onTrade(MarketDataMessage.Trade.Builder trade) {
        for (SubscriberState state : new LinkedList<>(subscribers.values())) {
            if (!state.subscribe.getTrade() || !canWrite(state)) {
                continue;
            }

            MarketDataMessage.Trade.Builder tradeNew = trade.clone();
            tradeNew.setId(state.subscribe.getId());
            tradeNew.setSymbol(state.subscribe.getSymbol());
            tradeNew.setAmount(trade.getAmount());
            tradeNew.setSettlType(state.subscribe.getSettlType());
            tradeNew.setSecurityExchange(state.subscribe.getSecurityExchange());
            tradeNew.setSecurityType(state.subscribe.getSecurityType());
            state.channel.write(tradeNew.build());
            MainApp.getOutboundWrites().increment();
        }
        flushChannels();
    }

    private boolean sendSnapshotIfNeeded(SubscriberState state, BookSnapshot snapshot) {
        if (snapshot == null || !snapshot.getReceivedSnapshot()) {
            return false;
        }

        boolean bookRequested = state.subscribe.getBook();
        boolean bookHasData = !snapshot.getBid().isEmpty() || !snapshot.getAsk().isEmpty();

        // Si el libro esta (aun) vacio, olvida cualquier snapshot de libro previo: cuando se
        // llene habra que reenviar un Snapshot completo, no un delta.
        if (bookRequested && !bookHasData) {
            state.bookSnapshotSent = false;
        }

        // (Re)envia un Snapshot cuando: el subscriber nunca recibio uno, O pidio libro, recibio
        // uno VACIO antes, y el libro ahora tiene puntas. El cliente pinta el ladder desde el
        // Snapshot y NO lo arma desde un IncrementalBook sobre un ladder vacio (causa raiz del
        // "no se ven las puntas de los CFI hasta resuscribir").
        boolean firstTime = !state.snapshotSent;
        boolean bookJustPopulated = bookRequested && !state.bookSnapshotSent && bookHasData;
        if (!firstTime && !bookJustPopulated) {
            return false;
        }
        if (!canWrite(state)) {
            return false;
        }

        MarketDataMessage.Snapshot.Builder msg = MarketDataMessage.Snapshot.newBuilder();
        msg.setId(state.subscribe.getId());
        msg.setSymbol(state.subscribe.getSymbol());
        msg.setSecurityExchange(state.subscribe.getSecurityExchange());
        msg.setSettlType(state.subscribe.getSettlType());

        if (state.subscribe.getTrade() && !snapshot.getTrades().isEmpty()) {
            for (MarketDataMessage.Trade trade : snapshot.getTrades()) {
                MarketDataMessage.Trade.Builder tradeBuilder = trade.toBuilder();
                tradeBuilder.setSymbol(state.subscribe.getSymbol());
                tradeBuilder.setSecurityExchange(state.subscribe.getSecurityExchange());
                tradeBuilder.setSettlType(state.subscribe.getSettlType());
                tradeBuilder.setId(state.subscribe.getId());
                msg.addTrades(tradeBuilder.build());
            }
        }

        if (state.subscribe.getStatistic()) {
            MarketDataMessage.Statistic.Builder statistic = snapshot.getStatistic().clone();
            statistic.setId(state.subscribe.getId());
            statistic.setSymbol(state.subscribe.getSymbol());
            statistic.setSecurityExchange(state.subscribe.getSecurityExchange());
            statistic.setSettlType(state.subscribe.getSettlType());
            msg.setStatistic(statistic);
        }

        if (state.subscribe.getBook()) {
            if (state.subscribe.getDepth().equals(MarketDataMessage.Depth.FULL_BOOK)) {
                msg.addAllBids(snapshot.getBid());
                msg.addAllAsks(snapshot.getAsk());
            } else if (!snapshot.getAsk().isEmpty() && !snapshot.getBid().isEmpty()) {
                msg.addAsks(snapshot.getAsk().getFirst());
                msg.addBids(snapshot.getBid().getFirst());
            }
        }

        state.channel.write(msg.build());
        MainApp.getOutboundWrites().increment();
        state.snapshotSent = true;
        if (bookHasData) {
            state.bookSnapshotSent = true;
        }
        state.droppedUpdates = 0;
        return true;
    }

    private void sendIncremental(SubscriberState state,
                                 MarketDataMessage.Statistic statistic,
                                 MarketDataMessage.IncrementalBook incremental) {
        if (!canWrite(state)) {
            return;
        }

        if (statistic != null) {
            state.channel.write(statistic);
            MainApp.getOutboundWrites().increment();
        }

        if (incremental != null) {
            state.channel.write(incremental);
            MainApp.getOutboundWrites().increment();
        }
        state.droppedUpdates = 0;
    }

    private void flushChannels() {
        Set<Channel> channels = new LinkedHashSet<>();
        for (SubscriberState state : new LinkedList<>(subscribers.values())) {
            if (!canWrite(state)) {
                continue;
            }
            channels.add(state.channel);
        }
        for (Channel channel : channels) {
            channel.flush();
        }
    }

    private Map<SubscriptionProfile, List<SubscriberState>> groupByProfile() {
        Map<SubscriptionProfile, List<SubscriberState>> grouped = new LinkedHashMap<>();
        for (SubscriberState state : new LinkedList<>(subscribers.values())) {
            if (!canWrite(state)) {
                continue;
            }
            SubscriptionProfile profile = SubscriptionProfile.from(state.subscribe);
            grouped.computeIfAbsent(profile, ignored -> new LinkedList<>()).add(state);
        }
        return grouped;
    }

    private MarketDataMessage.Statistic buildStatistic(BookSnapshot snapshot, SubscriberState state) {
        return buildStatistic(snapshot, state, SubscriptionProfile.from(state.subscribe));
    }

    private MarketDataMessage.Statistic buildStatistic(BookSnapshot snapshot,
                                                       SubscriberState state,
                                                       SubscriptionProfile profile) {
        if (snapshot == null || !profile.requiresStatistic) {
            return null;
        }
        MarketDataMessage.Statistic.Builder statistic = snapshot.getStatistic().clone();
        statistic.setId(state.subscribe.getId());
        statistic.setSymbol(state.subscribe.getSymbol());
        statistic.setSecurityExchange(state.subscribe.getSecurityExchange());
        statistic.setSettlType(state.subscribe.getSettlType());
        statistic.setSecurityType(state.subscribe.getSecurityType());
        return statistic.build();
    }

    private MarketDataMessage.IncrementalBook buildIncremental(BookSnapshot snapshot, SubscriberState state) {
        return buildIncremental(snapshot, state, SubscriptionProfile.from(state.subscribe));
    }

    private MarketDataMessage.IncrementalBook buildIncremental(BookSnapshot snapshot,
                                                               SubscriberState state,
                                                               SubscriptionProfile profile) {
        if (snapshot == null || !profile.requiresBook) {
            return null;
        }
        MarketDataMessage.IncrementalBook.Builder incremental = MarketDataMessage.IncrementalBook.newBuilder();
        incremental.setId(state.subscribe.getId());
        incremental.setSymbol(state.subscribe.getSymbol());
        incremental.setSecurityExchange(state.subscribe.getSecurityExchange());
        incremental.setSettlType(state.subscribe.getSettlType());
        incremental.setSecurityType(state.subscribe.getSecurityType());

        if (profile.depth.equals(MarketDataMessage.Depth.FULL_BOOK)) {
            incremental.addAllBids(snapshot.getBid());
            incremental.addAllAsks(snapshot.getAsk());
        } else if (!snapshot.getAsk().isEmpty() && !snapshot.getBid().isEmpty()) {
            incremental.addAsks(snapshot.getAsk().getFirst());
            incremental.addBids(snapshot.getBid().getFirst());
        }
        return incremental.build();
    }

    private boolean canWrite(SubscriberState state) {
        if (state.channel == null || !state.channel.isActive()) {
            if (subscribers.remove(state.subscriberKey) != null) {
                MainApp.getActiveTopicSubscribers().decrement();
            }
            stopIfEmpty();
            return false;
        }
        if (state.channel.isWritable()) {
            return true;
        }

        state.droppedUpdates++;
        MainApp.getOutboundSkipped().increment();
        if (state.droppedUpdates >= MAX_DROPPED_UPDATES_BEFORE_EVICT) {
            log.warn("evicting slow consumer topic={} clientSubId={} droppedUpdates={}",
                    topic, state.subscribe.getId(), state.droppedUpdates);
            if (subscribers.remove(state.subscriberKey) != null) {
                MainApp.getActiveTopicSubscribers().decrement();
                MainApp.getOutboundEvicted().increment();
            }
            stopIfEmpty();
        }
        return false;
    }

    private void stopIfEmpty() {
        if (subscribers.isEmpty()) {
            // Remove from the global map BEFORE scheduling the stop so that concurrent
            // reattachSubscription() calls see null in the map and create a fresh
            // TopicDistributor instead of sending AddSubscriber to a dying actor.
            MainApp.getTopicDistributorMaps().remove(topic, getSelf());
            getContext().stop(getSelf());
        }
    }

    @Data
    public static class AddSubscriber {
        private final String subscriberKey;
        private final ChannelHandlerContext ctx;
        private final MarketDataMessage.Subscribe subscribe;
    }

    @Data
    public static class RemoveSubscriber {
        private final String subscriberKey;
    }

    private static final class SubscriberState {
        private final String subscriberKey;
        private Channel channel;
        private MarketDataMessage.Subscribe subscribe;
        private boolean snapshotSent;
        private boolean bookSnapshotSent;
        private int droppedUpdates;

        private SubscriberState(String subscriberKey, Channel channel, MarketDataMessage.Subscribe subscribe) {
            this.subscriberKey = subscriberKey;
            this.channel = channel;
            this.subscribe = subscribe;
        }
    }

    private static final class SubscriptionProfile {
        private final String symbol;
        private final cl.vc.module.protocolbuff.routing.RoutingMessage.SettlType settlType;
        private final MarketDataMessage.SecurityExchangeMarketData securityExchange;
        private final MarketDataMessage.Depth depth;
        private final boolean requiresBook;
        private final boolean requiresStatistic;

        private SubscriptionProfile(String symbol,
                                    cl.vc.module.protocolbuff.routing.RoutingMessage.SettlType settlType,
                                    MarketDataMessage.SecurityExchangeMarketData securityExchange,
                                    MarketDataMessage.Depth depth,
                                    boolean requiresBook,
                                    boolean requiresStatistic) {
            this.symbol = symbol;
            this.settlType = settlType;
            this.securityExchange = securityExchange;
            this.depth = depth;
            this.requiresBook = requiresBook;
            this.requiresStatistic = requiresStatistic;
        }

        private static SubscriptionProfile from(MarketDataMessage.Subscribe subscribe) {
            return new SubscriptionProfile(
                    subscribe.getSymbol(),
                    subscribe.getSettlType(),
                    subscribe.getSecurityExchange(),
                    subscribe.getDepth(),
                    subscribe.getBook(),
                    subscribe.getStatistic());
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof SubscriptionProfile other)) {
                return false;
            }
            return requiresBook == other.requiresBook
                    && requiresStatistic == other.requiresStatistic
                    && symbol.equals(other.symbol)
                    && settlType.equals(other.settlType)
                    && securityExchange.equals(other.securityExchange)
                    && depth.equals(other.depth);
        }

        @Override
        public int hashCode() {
            int result = symbol.hashCode();
            result = 31 * result + settlType.hashCode();
            result = 31 * result + securityExchange.hashCode();
            result = 31 * result + depth.hashCode();
            result = 31 * result + Boolean.hashCode(requiresBook);
            result = 31 * result + Boolean.hashCode(requiresStatistic);
            return result;
        }
    }
}
