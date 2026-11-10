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

    // Собственный клиентский сертификат: права выдаются через ACL, суперпользователем он не является
    private static final Path CLIENT_CERTS = Paths.get("certificates/clients/producer");

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093,localhost:9094,localhost:9095");

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        props.put("security.protocol", "SSL");
        props.put("ssl.truststore.location",
                CLIENT_CERTS.resolve("kafka.client.truststore.jks").toAbsolutePath().toString());
        props.put("ssl.truststore.password", readPassword(CLIENT_CERTS.resolve("truststore.password")));
        props.put("ssl.keystore.location",
                CLIENT_CERTS.resolve("kafka.client.keystore.jks").toAbsolutePath().toString());
        props.put("ssl.keystore.password", readPassword(CLIENT_CERTS.resolve("keystore.password")));
        props.put("ssl.key.password", readPassword(CLIENT_CERTS.resolve("key.password")));

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