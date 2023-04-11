package io.kafka.streams.wikimedia;

import io.kafka.streams.wikimedia.processor.BotCountStreamBuilder;
import io.kafka.streams.wikimedia.processor.EventCountTimeseriesBuilder;
import io.kafka.streams.wikimedia.processor.WebsiteCountStreamBuilder;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class WikimediaStreamsApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(WikimediaStreamsApp.class);
    private static final Properties properties;
    private static final String INPUT_TOPIC = "wikimedia.recentchange";

    static {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wikimedia-stats-application");
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.plain.PlainLoginModule required username='shivaay-it' password='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiJzaGl2YWF5LWl0Iiwib3JnYW5pemF0aW9uSWQiOjcxMzgwLCJ1c2VySWQiOm51bGwsImZvckV4cGlyYXRpb25DaGVjayI6ImFlYTlkZjY2LWU4M2YtNDVlZC04ZTQ5LTE3YjQ0ZDAwODA0MCJ9fQ.afbo4K-QM1X7BaEUkHN9-YooKORipTYYZSp3KiTwXIE';\n");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    }

    public static void main(String[] args) {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> changeJsonStream = builder.stream(INPUT_TOPIC);

        BotCountStreamBuilder botCountStreamBuilder = new BotCountStreamBuilder(changeJsonStream);
        botCountStreamBuilder.setup();

        WebsiteCountStreamBuilder websiteCountStreamBuilder = new WebsiteCountStreamBuilder(changeJsonStream);
        websiteCountStreamBuilder.setup();

        EventCountTimeseriesBuilder eventCountTimeseriesBuilder = new EventCountTimeseriesBuilder(changeJsonStream);
        eventCountTimeseriesBuilder.setup();

        final Topology appTopology = builder.build();
        LOGGER.info("Topology: {}", appTopology.describe());
        KafkaStreams streams = new KafkaStreams(appTopology, properties);
        streams.start();
    }
}
