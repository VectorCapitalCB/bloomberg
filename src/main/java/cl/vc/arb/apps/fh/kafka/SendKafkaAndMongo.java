package cl.vc.arb.apps.fh.kafka;

import akka.actor.AbstractActor;
import akka.actor.Props;
import cl.vc.module.protocolbuff.notification.NotificationMessage;
import com.google.protobuf.util.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


@Slf4j
public class SendKafkaAndMongo extends AbstractActor {

    private JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields().omittingInsignificantWhitespace();

    private KafkaAdapter kafkaAdapter;

    private Boolean isConnected;

    private  Properties properties;


    private SendKafkaAndMongo(Properties properties) {
        this.properties = properties;
    }

    public static Props props(Properties properties) {
        return Props.create(SendKafkaAndMongo.class, properties);
    }

    @Override
    public void preStart() {
        try {

            if (isKafkaEnabled()) {
                kafkaAdapter = new KafkaAdapter(properties);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NotificationMessage.Notification.class, this::onNotification)
                .build();
    }


    private void onNotification(NotificationMessage.Notification msg) {
        try {

            String topic = resolveNotificationTopic(msg);
            String message = printer.print(msg);

            if (kafkaAdapter != null) {
                kafkaAdapter.getProducer().send(new ProducerRecord<>(topic, message), (metadata, exception) -> {
                    if (exception != null) {
                        log.warn("kafka notification send failed topic={} securityExchange={} reason={}",
                                topic, msg.getSecurityExchange(), exception.getMessage());
                    }
                });
            }


        } catch (Exception exc) {
            log.error(exc.getMessage(), exc);
        }
    }

    private boolean isKafkaEnabled() {
        String binderBrokers = properties.getProperty("app.stream.kafka.binder.brokers");
        if (binderBrokers != null && !binderBrokers.trim().isEmpty()) {
            return true;
        }
        return Boolean.parseBoolean(properties.getProperty("kafka.kafka.connect"));
    }

    private String resolveNotificationTopic(NotificationMessage.Notification msg) {
        String configuredTopic = properties.getProperty("app.stream.kafka.binder.topics");
        if (configuredTopic != null && !configuredTopic.trim().isEmpty()) {
            return configuredTopic.trim();
        }
        return "trade-monitor-notification-" + msg.getSecurityExchange();
    }

}
