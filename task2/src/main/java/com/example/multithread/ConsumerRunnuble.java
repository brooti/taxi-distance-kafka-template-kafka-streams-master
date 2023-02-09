package com.example.multithread;

import com.example.deserializer.VehicleDesirializer;
import com.example.model.VehicleSignal;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerRunnuble  implements Runnable{
    @Override
    public void run() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9095, localhost:9094, localhost:9093");
        properties.put("group.id", "taxi");
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", VehicleDesirializer.class.getName());

        KafkaConsumer<String, VehicleSignal> consumer1 = new KafkaConsumer<>(properties);

        consumer1.subscribe(Collections.singletonList("input"));

        while (true) {
            ConsumerRecords<String, VehicleSignal> records1 = consumer1.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, VehicleSignal> record : records1) {
                System.out.println("Consumer  "+ Thread.currentThread().getName() + ", received message: " + record.value());
            }
        }
    }
}
