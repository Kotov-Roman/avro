package com.epam;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.logging.Logger;

public class KafkaAvroConsumer {
    private final Properties properties;
    public final String topic;

    Logger logger = Logger.getLogger(KafkaAvroConsumer.class.getName());

    public KafkaAvroConsumer(String topic) {
        this.topic = topic;
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-consumer-group-v1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // avro part (deserializer)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        properties.setProperty("schema.registry.url", "http://localhost:8081");
        this.properties = properties;
    }

    public void consume() {
        try (KafkaConsumer<String, GenericRecord> kafkaConsumer = new KafkaConsumer<>(properties)) {
            kafkaConsumer.subscribe(Collections.singleton(topic));
            while (true) {
                System.out.println("Polling");
                ConsumerRecords<String, GenericRecord> records = kafkaConsumer.poll(1000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    logger.info(Thread.currentThread().getName() + "received: " + record.value());
                }
            }
        }

    }
}
