package com.epam;

import com.example.Customer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaAvroProducer {
    private final KafkaProducer<String, Customer> kafkaProducer;
    public final String topic;

    Logger logger = Logger.getLogger(KafkaAvroProducer.class.getName());


    public KafkaAvroProducer(String topic) {
        this.topic = topic;

        Properties properties = new Properties();
        // normal producer
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
//        properties.setProperty("acks", "all");
//        properties.setProperty("retries", "10");
        // avro part
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, Customer> producer = new KafkaProducer<>(properties);

        this.kafkaProducer = producer;
    }

    public void produce(int age) {
        Customer customer = Customer.newBuilder()
                .setAge(age)
                .setAutomatedEmail(false)
                .setFirstName("John")
                .setLastName("Doe")
                .setHeight(178f)
                .setWeight(75f)
                .build();

        ProducerRecord<String, Customer> producerRecord = new ProducerRecord<String, Customer>(
                topic, customer
        );

        System.out.println(customer);
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info(Thread.currentThread().getName() + "metadata = " + metadata.toString());
            } else {
                exception.printStackTrace();
            }
        });
    }
}
