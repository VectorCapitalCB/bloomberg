package cl.vc.arb.apps.fh.tcp;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.arb.apps.fh.MainApp;
import cl.vc.arb.apps.fh.bs.SessionTracker;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ClientManager extends AbstractActor {


    public static Props props() {
        return Props.create(ClientManager.class);
    }

    @Override
    public void postStop() {
        log.info("se elimina el cliente manager ");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .build();
    }

    private void onMessages(TransportingObjects conn) {
        try {
            String channelId = conn.getCtx().channel().id().toString();
            String sessionId = resolveSessionId(conn);
            String messageType = conn.getMessage() == null ? "null" : conn.getMessage().getClass().getSimpleName();
            log.debug("client manager received message channel={} session={} type={} trackedSession={}",
                    channelId, sessionId, messageType, MainApp.sessionTrackerMaps.containsKey(sessionId));

            if(conn.getMessage() instanceof SessionsMessage.Connect){
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect){
                onDisconnect(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Subscribe){
                getOrCreateSessionTracker(conn).tell(conn.getMessage(), getSelf());

            } else if (conn.getMessage() instanceof MarketDataMessage.SecurityListRequest){
                getOrCreateSessionTracker(conn).tell(conn.getMessage(), getSelf());

            } else if (conn.getMessage() instanceof MarketDataMessage.Unsubscribe){
                getOrCreateSessionTracker(conn).tell(conn.getMessage(), getSelf());
            }


        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void onConnect(TransportingObjects conn) {
        try {
            String channelId = conn.getCtx().channel().id().toString();
            String sessionId = resolveSessionId(conn);
            String rawId = (conn.getMessage() instanceof SessionsMessage.Connect c) ? c.getId() : "";
            MainApp.getChannelSessionMaps().put(channelId, sessionId);
            log.info("client connect channel={} session={} rawId={} existingSessionTracker={}",
                    channelId, sessionId, rawId, MainApp.sessionTrackerMaps.containsKey(sessionId));

            if (!MainApp.sessionTrackerMaps.containsKey(sessionId)) {
                ActorRef clientManagerPool = MainApp.getSystem().actorOf(SessionTracker.props(sessionId, conn.getCtx()));
                MainApp.sessionTrackerMaps.put(sessionId, clientManagerPool);
                log.info("client connect created session tracker channel={} session={} actor={}",
                        channelId, sessionId, clientManagerPool);
            } else {
                ActorRef sessionTracker = MainApp.sessionTrackerMaps.get(sessionId);
                sessionTracker.tell(new SessionTracker.RebindConnection(conn.getCtx()), getSelf());
                log.info("client connect rebound session tracker channel={} session={} actor={}",
                        channelId, sessionId, sessionTracker);
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {

            String channelId = conn.getCtx().channel().id().toString();
            String sessionId = resolveSessionId(conn);
            MainApp.getChannelSessionMaps().remove(channelId);
            int subscriptions = MainApp.getActorsSubscribe().containsKey(sessionId)
                    ? MainApp.getActorsSubscribe().get(sessionId).size() : -1;
            log.info("client disconnect channel={} session={} tracked={} subscriptions={}",
                    channelId, sessionId, MainApp.sessionTrackerMaps.containsKey(sessionId),
                    subscriptions);

            if (MainApp.sessionTrackerMaps.containsKey(sessionId)) {
                ActorRef client = MainApp.sessionTrackerMaps.get(sessionId);
                if (client != null) {
                    client.tell(new SessionTracker.ConnectionLost(channelId), getSelf());
                    log.info("client disconnect preserved session tracker channel={} session={} actor={} subscriptionsKept={}",
                            channelId, sessionId, client, subscriptions);
                }
            }

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    private ActorRef getOrCreateSessionTracker(TransportingObjects conn) {
        String channelId = conn.getCtx().channel().id().toString();
        String sessionId = MainApp.getChannelSessionMaps().getOrDefault(channelId, channelId);
        return MainApp.sessionTrackerMaps.computeIfAbsent(sessionId, ignored -> {
            ActorRef sessionTracker = MainApp.getSystem().actorOf(SessionTracker.props(sessionId, conn.getCtx()));
            MainApp.getChannelSessionMaps().put(channelId, sessionId);
            log.info("client manager created missing session tracker for channel={} session={} actor={}",
                    channelId, sessionId, sessionTracker);
            return sessionTracker;
        });
    }

    private String resolveSessionId(TransportingObjects conn) {
        String channelId = conn.getCtx().channel().id().toString();
        if (conn.getMessage() instanceof SessionsMessage.Connect connect) {
            // Prefer username when it is a real client identity. Some clients send
            // username=NONE, which must not collapse all TCP clients into one session.
            if (isUsableSessionId(connect.getUsername())) {
                log.debug("client connect resolve session by username channel={} username={} id={}",
                        channelId, connect.getUsername(), connect.getId());
                return connect.getUsername();
            }
            if (isUsableSessionId(connect.getId())) {
                return connect.getId();
            }
        }
        // For Disconnect and all other messages, look up stable session by channelId
        return MainApp.getChannelSessionMaps().getOrDefault(channelId, channelId);
    }

    private boolean isUsableSessionId(String value) {
        return value != null
                && !value.isBlank()
                && !"NONE".equalsIgnoreCase(value.trim())
                && !"NULL".equalsIgnoreCase(value.trim());
    }

}

