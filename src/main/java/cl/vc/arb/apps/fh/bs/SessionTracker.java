package cl.vc.arb.apps.fh.bs;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

import static cl.vc.arb.apps.fh.MainApp.securityList;
import static cl.vc.arb.apps.fh.utils.Helper.LOGON_FIX;
import static cl.vc.arb.apps.fh.utils.Helper.TRADE_GENERAL;

@Slf4j
public class SessionTracker extends AbstractActor {

    private ChannelHandlerContext ctx;

    private final String idConnect;
    private final Map<String, MarketDataMessage.Subscribe> activeSubscriptions = new HashMap<>();

    public SessionTracker(String idConnect, ChannelHandlerContext ctx) {
        this.idConnect = idConnect;
        this.ctx = ctx;
    }

    public static Props props(String idConnect, ChannelHandlerContext ctx) {
        return Props.create(SessionTracker.class, idConnect, ctx);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(MarketDataMessage.Subscribe.class, this::onSubscribe)
                .match(MarketDataMessage.Unsubscribe.class, this::unsubscribeMarketData)
                .match(MarketDataMessage.SecurityListRequest.class, this::onNewSecurityListRequest)
                .match(MarketDataMessage.News.class, this::onNews)
                .match(MarketDataMessage.Trade.class, this::onGlobalTrade)
                .match(NotificationMessage.Notification.class, this::onNotification)
                .match(SessionsMessage.Disconnect.class, this::onDisconnectSessionFix)
                .match(SessionsMessage.Connect.class, this::onConnectSessionFix)
                .match(RebindConnection.class, this::onRebindConnection)
                .match(ConnectionLost.class, this::onConnectionLost)
                .build();
    }


    @Override
    public void preStart() {
        try {
            
            MainApp.getActorsSubscribe().computeIfAbsent(idConnect, ignored -> new HashMap<>());

            log.info("session tracker start channel={} actor={} subscriptionsMapCreated={}",
                    resolveChannelId(), getSelf(), MainApp.getActorsSubscribe().containsKey(idConnect));
            MainApp.getEventBus().subscribe(getSelf(), LOGON_FIX);
            MainApp.getEventBus().subscribe(getSelf(), TRADE_GENERAL);

            MarketDataMessage.SnapshotNews.Builder news = MarketDataMessage.SnapshotNews.newBuilder();

            MainApp.getNews().forEach(news::addNews);

            writeToChannel(news.build());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    @Override
    public void postStop() {
        try {
            MainApp.getEventBus().unsubscribe(getSelf(), LOGON_FIX);
            MainApp.getEventBus().unsubscribe(getSelf(), TRADE_GENERAL);

            Map<String, ActorRef> subscriptions = MainApp.getActorsSubscribe().get(idConnect);
            if (subscriptions != null) {
                subscriptions.forEach((subscribeId, actorRef) ->
                        actorRef.tell(new TopicDistributor.RemoveSubscriber(buildSubscriberKey(subscribeId)), getSelf()));
                subscriptions.clear();
            }

            int remainingSubscriptions = subscriptions != null ? subscriptions.size() : -1;
            log.info("session tracker stop channel={} actor={} remainingSubscriptions={}",
                    resolveChannelId(), getSelf(), remainingSubscriptions);
            MainApp.getActorsSubscribe().remove(idConnect);
            MainApp.sessionTrackerMaps.remove(idConnect, getSelf());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    private void onNews(MarketDataMessage.News news) {
        try {
            writeToChannel(news);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    /** Forwards a global market trade (from any symbol) to the client channel. */
    private void onGlobalTrade(MarketDataMessage.Trade trade) {
        try {
            writeToChannel(trade);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    private void onConnectSessionFix(SessionsMessage.Connect not) {
        try {
            log.info("session tracker received FIX connect session={} channel={} actor={}",
                    idConnect, resolveChannelId(), getSelf());
            writeToChannel(not);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onNotification(NotificationMessage.Notification not) {
        try {

            writeToChannel(not);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onDisconnectSessionFix(SessionsMessage.Disconnect dis) {
        try {
            log.info("session tracker received FIX disconnect session={} channel={} actor={}",
                    idConnect, resolveChannelId(), getSelf());
            writeToChannel(dis);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    private void onSubscribe(MarketDataMessage.Subscribe subs) {

        try {
            // Normaliza el securityType contra la SecurityList de la bolsa: si el cliente pide un
            // securityType que el papel no tiene (p.ej. CS sobre un CFI), se sirve del stream canonico.
            // Asi CFI y CS del mismo papel resuelven al MISMO topic -> un solo stream -> sin trades duplicados.
            subs = normalizeSecurityType(subs);
            activeSubscriptions.put(subs.getId(), subs);
            Map<String, ActorRef> subscriptions = MainApp.getActorsSubscribe().get(idConnect);
            if (subscriptions == null) {
                subscriptions = new HashMap<>();
                MainApp.getActorsSubscribe().put(idConnect, subscriptions);
            }

            log.debug("session subscribe request channel={} clientSubId={} symbol={} exchange={} book={} trade={} statistic={}",
                    idConnect, subs.getId(), subs.getSymbol(), subs.getSecurityExchange(),
                    subs.getBook(), subs.getTrade(), subs.getStatistic());
            log.debug("session subscribe channel={} clientSubId={} existing={}",
                    idConnect, subs.getId(), subscriptions.containsKey(subs.getId()));
            log.debug("session subscribe actor map channel={} size={} actor={}",
                    idConnect, subscriptions.size(), getSelf());

            String topic = cl.vc.module.protocolbuff.generator.TopicGenerator.getTopicMKD(subs);
            ActorRef topicActor = MainApp.getTopicDistributorMaps().get(topic);
            if (topicActor == null) {
                synchronized (MainApp.getTopicDistributorMaps()) {
                    topicActor = MainApp.getTopicDistributorMaps().get(topic);
                    if (topicActor == null) {
                        topicActor = MainApp.getSystem().actorOf(TopicDistributor.props(subs));
                        MainApp.getTopicDistributorMaps().put(topic, topicActor);
                    }
                }
            }

            if (subscriptions.containsKey(subs.getId())) {
                ActorRef currentActor = subscriptions.get(subs.getId());
                if (!currentActor.equals(topicActor)) {
                    currentActor.tell(new TopicDistributor.RemoveSubscriber(buildSubscriberKey(subs.getId())), getSelf());
                    subscriptions.put(subs.getId(), topicActor);
                }
            } else {
                subscriptions.put(subs.getId(), topicActor);
            }

            topicActor.tell(new TopicDistributor.AddSubscriber(buildSubscriberKey(subs.getId()), ctx, subs), ActorRef.noSender());
            log.info("session registered subscription channel={} clientSubId={} topic={} distributor={}",
                    idConnect, subs.getId(), topic, topicActor);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    private void unsubscribeMarketData(MarketDataMessage.Unsubscribe msg) {
        try {
            Map<String, ActorRef> subscriptions = MainApp.getActorsSubscribe().get(idConnect);
            activeSubscriptions.remove(msg.getId());
            log.info("session unsubscribe request channel={} clientSubId={}", idConnect, msg.getId());
            log.info("session unsubscribe channel={} clientSubId={} exists={}",
                    idConnect, msg.getId(), subscriptions != null && subscriptions.containsKey(msg.getId()));

            if(subscriptions != null && subscriptions.containsKey(msg.getId())){
                ActorRef actorRef = subscriptions.get(msg.getId());
                actorRef.tell(new TopicDistributor.RemoveSubscriber(buildSubscriberKey(msg.getId())), getSelf());
                subscriptions.remove(msg.getId());
                log.info("session unsubscribe sent remove subscriber channel={} clientSubId={} actor={}",
                        resolveChannelId(), msg.getId(), actorRef);

            }

        } catch (Exception e) {
            sendReject(ctx != null ? ctx.channel() : null, msg.getId(), e.getMessage());
            log.error(e.getMessage(), e);
        }
    }


    private void onNewSecurityListRequest(MarketDataMessage.SecurityListRequest msg) {
        try {

            writeToChannel(securityList.setId(msg.getId()).build());

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private void sendReject(Channel channel, String id, String text) {
        try {

            MarketDataMessage.Rejected rejected = MarketDataMessage.Rejected.newBuilder()
                    .setReason("Failed")
                    .setText(text)
                    .setId(id)
                    .build();

            if (channel != null && channel.isActive()) {
                channel.writeAndFlush(rejected);
            } else {
                log.warn("session reject skipped session={} id={} channelActive=false", idConnect, id);
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private String buildSubscriberKey(String subscribeId) {
        return idConnect + "|" + subscribeId;
    }

    /**
     * Si el securityType pedido por el cliente no coincide con el de la SecurityList de la bolsa,
     * lo reescribe al canonico (loguea y sirve). El topic se calcula con el securityType canonico,
     * de modo que CFI y CS del mismo papel caen en el MISMO topic y no se duplica el stream.
     */
    private MarketDataMessage.Subscribe normalizeSecurityType(MarketDataMessage.Subscribe subs) {
        try {
            MarketDataMessage.Security sec =
                    MainApp.securityListMaps.get(subs.getSymbol() + subs.getSecurityExchange().name());
            if (sec == null) {
                return subs;
            }
            String canonical = sec.getSecurityType();
            if (canonical == null || canonical.isBlank()
                    || subs.getSecurityType().name().equalsIgnoreCase(canonical)) {
                return subs;
            }
            cl.vc.module.protocolbuff.routing.RoutingMessage.SecurityType canonEnum;
            try {
                canonEnum = cl.vc.module.protocolbuff.routing.RoutingMessage.SecurityType.valueOf(canonical);
            } catch (IllegalArgumentException ex) {
                log.warn("session securityType canonico desconocido symbol={} canonical={} (se sirve tal cual)",
                        subs.getSymbol(), canonical);
                return subs;
            }
            log.warn("session securityType mismatch NORMALIZADO symbol={} clientSubId={} requested={} canonical={} (se sirve del stream canonico)",
                    subs.getSymbol(), subs.getId(), subs.getSecurityType().name(), canonical);
            return subs.toBuilder().setSecurityType(canonEnum).build();
        } catch (Exception e) {
            log.error("normalizeSecurityType error symbol={}", subs.getSymbol(), e);
            return subs;
        }
    }

    private void onRebindConnection(RebindConnection msg) {
        try {
            this.ctx = msg.getCtx();
            MainApp.getChannelSessionMaps().put(resolveChannelId(), idConnect);
            log.info("session tracker rebound session={} channel={} activeSubscriptions={}",
                    idConnect, resolveChannelId(), activeSubscriptions.size());

            MarketDataMessage.SnapshotNews.Builder news = MarketDataMessage.SnapshotNews.newBuilder();
            MainApp.getNews().forEach(news::addNews);
            writeToChannel(news.build());

            // The client re-sends all its subscriptions automatically after TCP reconnect,
            // which are processed via onSubscribe() using the new ctx above.
            // Looping 28,846+ activeSubscriptions here would block this actor thread for
            // 10-30 seconds and delay the client's own re-subscription messages.

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private void onConnectionLost(ConnectionLost msg) {
        String currentChannelId = resolveChannelId();
        if (!msg.getChannelId().equals(currentChannelId)) {
            log.info("session tracker connection lost ignored - stale disconnect channel={} current={} session={}",
                    msg.getChannelId(), currentChannelId, idConnect);
            return;
        }
        log.info("session tracker connection lost session={} channel={} activeSubscriptions={}",
                idConnect, currentChannelId, activeSubscriptions.size());
        this.ctx = null;
    }

    private void reattachSubscription(MarketDataMessage.Subscribe subs) {
        Map<String, ActorRef> subscriptions = MainApp.getActorsSubscribe().computeIfAbsent(idConnect, ignored -> new HashMap<>());
        String topic = cl.vc.module.protocolbuff.generator.TopicGenerator.getTopicMKD(subs);
        ActorRef topicActor = MainApp.getTopicDistributorMaps().get(topic);
        if (topicActor == null) {
            synchronized (MainApp.getTopicDistributorMaps()) {
                topicActor = MainApp.getTopicDistributorMaps().get(topic);
                if (topicActor == null) {
                    topicActor = MainApp.getSystem().actorOf(TopicDistributor.props(subs));
                    MainApp.getTopicDistributorMaps().put(topic, topicActor);
                }
            }
        }
        subscriptions.put(subs.getId(), topicActor);
        topicActor.tell(new TopicDistributor.AddSubscriber(buildSubscriberKey(subs.getId()), ctx, subs), ActorRef.noSender());
        log.debug("session reattached subscription session={} channel={} clientSubId={} topic={} distributor={}",
                idConnect, resolveChannelId(), subs.getId(), topic, topicActor);
    }

    private void writeToChannel(Object message) {
        if (ctx == null || ctx.channel() == null || !ctx.channel().isActive()) {
            log.info("session write skipped session={} channel={} messageType={}",
                    idConnect, resolveChannelId(), message.getClass().getSimpleName());
            return;
        }
        ctx.channel().writeAndFlush(message);
    }

    private String resolveChannelId() {
        return ctx != null && ctx.channel() != null ? ctx.channel().id().toString() : "no-channel";
    }

    @Data
    public static class RebindConnection {
        private final ChannelHandlerContext ctx;
    }

    @Data
    public static class ConnectionLost {
        private final String channelId;
    }


}

