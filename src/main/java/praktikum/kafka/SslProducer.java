package praktikum.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class SslProducer {

    private static final Path BROKER_CERTS = Paths.get("certificates/brokers/kafka-1");

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location",
                BROKER_CERTS.resolve("kafka.server.truststore.jks").toAbsolutePath().toString());
        props.put("ssl.truststore.password", readPassword(BROKER_CERTS.resolve("truststore.password")));
        props.put("ssl.keystore.location",
                BROKER_CERTS.resolve("kafka.server.keystore.jks").toAbsolutePath().toString());
        props.put("ssl.keystore.password", readPassword(BROKER_CERTS.resolve("keystore.password")));
        props.put("ssl.key.password", readPassword(BROKER_CERTS.resolve("key.password")));

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String topic = args.length > 0 ? args[0] : "topic-1";
            for (int i = 0; i < 5; i++) {
                String key = "key-" + i;
                String value = "encrypted-message-" + i;
                producer.send(new ProducerRecord<>(topic, key, value),
                        (metadata, exception) -> {
                            if (exception == null) {
                                System.out.printf("Sent to %s partition=%d offset=%d%n",
                                        metadata.topic(), metadata.partition(), metadata.offset());
                            } else {
                                System.err.println("Send failed: " + exception.getMessage());
                            }
                        });
            }
            producer.flush();
        }
    }

    private static String readPassword(Path file) throws IOException {
        return Files.readString(file).trim();
    }
}