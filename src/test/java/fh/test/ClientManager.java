package fh.test;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import cl.vc.module.protocolbuff.generator.IDGenerator;
import cl.vc.module.protocolbuff.mkd.MarketDataMessage;
import cl.vc.module.protocolbuff.routing.RoutingMessage;
import cl.vc.module.protocolbuff.session.SessionsMessage;
import cl.vc.module.protocolbuff.tcp.TransportingObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ClientManager extends AbstractActor {

    private final BiMap<String, ActorRef> trackerMapsChannel = HashBiMap.create();

    public static Props props() {
        return Props.create(ClientManager.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(TransportingObjects.class, this::onMessages)
                .build();
    }

    private void onMessages(TransportingObjects conn) {
        try {


            if(conn.getMessage() instanceof SessionsMessage.Connect){
                onConnect(conn);

            } else if (conn.getMessage() instanceof SessionsMessage.Disconnect){
                onDisconnect(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.NewOrderRequest){
                onNewOrderRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderReplaceRequest){
                onOrderReplaceRequest(conn);

            } else if (conn.getMessage() instanceof RoutingMessage.OrderCancelRequest){
                onCancelOrderRequest(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.SecurityList){
                onSecurityList(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Snapshot){
                onSnapshot(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Ohlcv){
                onOhlcv(conn);

            } else if (conn.getMessage() instanceof MarketDataMessage.Statistic){
                onStatistic(conn);

            }



        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }


    public void onStatistic( TransportingObjects msg) {
        try {

            String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(msg.getMessage());
            jsonString = jsonString.replace("\n", "").replace("\r", ""); // Elimina saltos de línea y retorno de carro
            System.out.println(msg.getMessage().getDescriptorForType().getName() + ":" + jsonString);

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onOhlcv( TransportingObjects msg) {
        try {

            String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(msg.getMessage());
            jsonString = jsonString.replace("\n", "").replace("\r", ""); // Elimina saltos de línea y retorno de carro
            System.out.println(msg.getMessage().getDescriptorForType().getName() + ":" + jsonString);



        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onSnapshot( TransportingObjects msg) {
        try {

            String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(msg.getMessage());
            jsonString = jsonString.replace("\n", "").replace("\r", ""); // Elimina saltos de línea y retorno de carro
            System.out.println(msg.getMessage().getDescriptorForType().getName() + ":" + jsonString);


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onIncremental(TransportingObjects msg) {
        try {

            String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(msg.getMessage());
            jsonString = jsonString.replace("\n", "").replace("\r", ""); // Elimina saltos de línea y retorno de carro
            System.out.println(msg.getMessage().getDescriptorForType().getName() + ":" + jsonString);

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onSecurityList(TransportingObjects msg) {
        try {

            String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(msg.getMessage());
            jsonString = jsonString.replace("\n", "").replace("\r", ""); // Elimina saltos de línea y retorno de carro
            System.out.println(msg.getMessage().getDescriptorForType().getName() + ":" + jsonString);

        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onConnect(TransportingObjects conn) {
        try {

            log.info(" id : {} ",conn.getCtx().channel().id().toString());

            Thread.sleep(2000);

            MarketDataMessage.SecurityListRequest test =
                    MarketDataMessage.SecurityListRequest.newBuilder().setId(IDGenerator.getID()).build();

            FHAppTest.client1.sendMessage(test);

            Thread.sleep(2000);

            MarketDataMessage.Subscribe subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setTrade(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .setId(IDGenerator.getID())
                    .setSettlType(RoutingMessage.SettlType.T2)
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .setStatistic(true)
                    .setSymbol("SQM-B").build();

            FHAppTest.client1.sendMessage(subscribe);


            Thread.sleep(200);

            subscribe = MarketDataMessage.Subscribe.newBuilder()
                    .setTrade(true)
                    .setDepth(MarketDataMessage.Depth.TOP_OF_THE_BOOK)
                    .setId(IDGenerator.getID())
                    .setSettlType(RoutingMessage.SettlType.T2)
                    .setSecurityExchange(MarketDataMessage.SecurityExchangeMarketData.BCS)
                    .setStatistic(true)
                    .setSymbol("LTM").build();

            //FHAppTest.client1.sendMessage(subscribe);


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onDisconnect(TransportingObjects conn) {
        try {


        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onNewOrderRequest(TransportingObjects conn) {
        try {
            trackerMapsChannel.get(conn.getCtx().channel().id()).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void onOrderReplaceRequest(TransportingObjects conn) {
        try {
            trackerMapsChannel.get(conn.getCtx().channel().id()).tell(conn.getMessage(), ActorRef.noSender());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }


    public void onCancelOrderRequest(TransportingObjects conn) {
        try {
            trackerMapsChannel.get(conn.getCtx().channel().id()).tell(conn.getMessage(), getSelf());
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

}
