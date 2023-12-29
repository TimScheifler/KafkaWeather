package org.example.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaWeatherConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWeatherConsumer.class);

    public static void main(String[] args) {
        String groupId = "my-first-application";
        String topic = "weather";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);

        properties.setProperty("auto.offset.reset", "earliest"); //none -> if we dont have any existing consumer group, we fail. earliest -> read from the beginning of the topic. latest -> from now.

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(topic));

        while (true) {

            LOG.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(30000));

            for (ConsumerRecord<String, String> record: records) {
                LOG.info("Key" + record.key() + " | Value: " + record.value());
                LOG.info("Partition" + record.partition() + " | Offset: " + record.offset());
            }
        }
    }
}
