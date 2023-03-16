import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class producers {
    private static final Logger log = LoggerFactory.getLogger(producers.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");
        String bootstrapServers = "127.0.0.1:9092";
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"4J7t9tJjOjMwRlHN86hH0r\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI0Sjd0OXRKak9qTXdSbEhOODZoSDByIiwib3JnYW5pemF0aW9uSWQiOjcxMzgwLCJ1c2VySWQiOjgyNzM1LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIzYzc0NWEzOC1jNTU0LTRlNTQtYWZjOC05YzllNzc2YmYzZjAifX0.qNWeuRvdj3U9gq-AySXTCctQ-jGb-CAk4bgj-_OChMw\";");
        properties.setProperty("sasl.mechanism","PLAIN");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        Callback cb = (recordMetadata, e) -> {
            // executes every time a record is successfully sent or an exception is thrown
            if (e == null) {
                // the record was successfully sent
                log.info("Received new metadata. \n" +
                        "Topic:" + recordMetadata.topic() + "\n" +
                      //  "Key:" + producerRecord.key() + "\n" +
                        "Partition: " + recordMetadata.partition() + "\n" +
                        "Offset: " + recordMetadata.offset() + "\n" +
                        "Timestamp: " + recordMetadata.timestamp());
            } else {
                log.error("Error while producing", e);
            }
        };

        for(int b=0;b<10;b++) {
            for (int i = 0; i < 10; i++) {
                // create a producer record
                String topic = "first_topic";
                String value = "kafka world " + i +" batch "+b;
                String key = "id_" + i;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data - asynchronous
                producer.send(producerRecord, cb);
            }
        }

        // flush data - synchronous
        producer.flush();

        // flush and close producer
        producer.close();
    }
}
