package cl.vc.arb.apps.fh.kafka;

import cl.vc.module.protocolbuff.generator.IDGenerator;
import lombok.Data;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@lombok.extern.slf4j.Slf4j
@Data
public class KafkaAdapter extends Thread {

    private Properties properties;

    private KafkaProducer<String, String> producer;

    public KafkaAdapter(Properties properties) {

        try {

            String groupId = firstNonBlank(
                    properties.getProperty("app.stream.kafka.binder.group"),
                    properties.getProperty("kafka.group"),
                    "orb-bcs"
            );
            String bootstrapServers = firstNonBlank(
                    properties.getProperty("app.stream.kafka.binder.brokers"),
                    properties.getProperty("kafka.broker")
            );
            String autoOffsetReset = firstNonBlank(
                    properties.getProperty("app.stream.kafka.binder.offset.reset"),
                    properties.getProperty("kafka.reset"),
                    "latest"
            );

            this.properties = new Properties();
            this.properties.put("bootstrap.servers", bootstrapServers);
            this.properties.put("enable.auto.commit", "false");
            this.properties.put("auto.commit.interval.ms", "1000");
            this.properties.put("session.timeout.ms", "30000");
            this.properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            this.properties.put("auto.offset.reset", autoOffsetReset);

            if ("earliest".equalsIgnoreCase(this.properties.get("auto.offset.reset").toString())) {
                groupId += "-" + IDGenerator.getID();
            }

            this.properties.put("group.id", groupId);

            Properties producerProps = new Properties();
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,
                    firstNonBlank(properties.getProperty("kafka.producer.max.block.ms"), "3000"));
            producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                    firstNonBlank(properties.getProperty("kafka.producer.request.timeout.ms"), "3000"));
            producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
                    firstNonBlank(properties.getProperty("kafka.producer.delivery.timeout.ms"), "10000"));
            producerProps.put(ProducerConfig.RETRIES_CONFIG,
                    firstNonBlank(properties.getProperty("kafka.producer.retries"), "1"));
            producer = new KafkaProducer<>(producerProps);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static String firstNonBlank(String... values) {
        for (String value : values) {
            if (value != null && !value.trim().isEmpty()) {
                return value.trim();
            }
        }
        return null;
    }

}
