package com.example.service;

import com.example.model.TaxiDistanceInfo;
import com.example.model.VehicleSignal;
import com.example.multithread.ConsumerRunnuble;
import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;

import org.apache.kafka.streams.StreamsBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class VehicleSignalsKafkaProcessor {

    @Value("${spring.kafka.vehicle.signals.vehicle-signals-topic}")
    private String vehicleSignalsTopic;
    @Value("${spring.kafka.vehicle.traveled-distance-topic}")
    private String traveledDistanceTopic;

    private final Map<String, Double> distanceStorage = new ConcurrentHashMap<>();
    private final Map<String, VehicleSignal> lastReceivedSignals = new ConcurrentHashMap<>();

    @PostConstruct
    public void processSignalsByDistance() {
        ConsumerRunnuble runuble = new ConsumerRunnuble();
        Thread thread = new Thread(runuble);
        thread.start();
        Thread thread2 = new Thread(runuble);
        thread2.start();
        Thread thread3 = new Thread(runuble);
        thread3.start();
    }

    private TaxiDistanceInfo updateTaxiDistanceInfo(VehicleSignal vehicleSignal) {
        String id = vehicleSignal.getId();
        if (lastReceivedSignals.containsKey(id)) {
            return createTaxiDistanceInfo(vehicleSignal);
        }
        return createDefaultInfo(vehicleSignal);
    }

    private TaxiDistanceInfo createDefaultInfo(VehicleSignal vehicleSignal) {
        String id = vehicleSignal.getId();
        lastReceivedSignals.put(id, vehicleSignal);
        distanceStorage.put(id, 0.0);
        return new TaxiDistanceInfo(id, 0);
    }

    private TaxiDistanceInfo createTaxiDistanceInfo(VehicleSignal vehicleSignal) {
        String id = vehicleSignal.getId();
        VehicleSignal prevSignal = lastReceivedSignals.get(id);
        double distanceSoFar = distanceStorage.get(id);
        double checkInDistance = calculateDistance(prevSignal, vehicleSignal);
        double newDistanceSoFar = distanceSoFar + checkInDistance;
        lastReceivedSignals.put(id, vehicleSignal);
        distanceStorage.put(id, newDistanceSoFar);
        return new TaxiDistanceInfo(id, newDistanceSoFar);
    }

    private double calculateDistance(VehicleSignal prev, VehicleSignal curr) {
        double a = prev.getLongitude() - curr.getLongitude();
        double b = prev.getLatitude() - curr.getLatitude();
        return Math.sqrt(a * a + b * b);
    }

}
