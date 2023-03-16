package com.epam;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

    public static void main(String[] args) {
        String topic = "customers-topic";
        KafkaAvroConsumer kafkaAvroConsumer = new KafkaAvroConsumer(topic);
        KafkaAvroProducer kafkaAvroProducer = new KafkaAvroProducer(topic);

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(2);

        scheduledExecutorService.scheduleAtFixedRate(() -> {
            kafkaAvroProducer.produce(999);
        }, 5, 1, TimeUnit.SECONDS);


        scheduledExecutorService.submit(kafkaAvroConsumer::consume);
    }
}