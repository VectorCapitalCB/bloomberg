package fh.test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.routing.RoundRobinPool;
import cl.vc.module.protocolbuff.tcp.NettyProtobufClient;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FHAppTest {

    public static NettyProtobufClient client1;

    public static void main(String[] args) {
        try {

            init(args);

        } catch (Exception ex) {
            log.error("Cannot start Application...", ex);
        }
    }

    public static void init(String[] args) {
        try {

            ActorSystem system = ActorSystem.create("fh-system");

            ActorRef clientManager = system.actorOf(new RoundRobinPool(100).props(ClientManager.props()), "client-manager");
           // client1 = new NettyProtobufClient("localhost:5200", clientManager, "logs","clientfh");
           // new Thread(client1).start();

            while (true){

            }

        } catch (Exception ex) {
            log.error("Cannot init FH", ex);
        }
    }

}
