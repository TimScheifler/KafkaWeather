package org.example.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaWeatherProducer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWeatherProducer.class);
    private final KafkaProducer<String, String> producer;

    public KafkaWeatherProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public void prouce(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

        producer.send(record, (metadata, exception) -> {
            // executed every time a record is successfully sent or an exception is thrown
            if (exception == null) {
                // successfully sent
                LOG.info("Key: " + key + " | Partition: " + metadata.partition());
            } else {
                LOG.error("Error", exception);
            }
        });
    }

}