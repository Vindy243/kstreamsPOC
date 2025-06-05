package com.lowes.kstreams.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lowes.kstreams.RetailSalesApp;
import com.lowes.kstreams.model.Product;
import com.lowes.kstreams.model.Purchase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Properties;
import java.util.Random;

@SpringBootApplication
@RestController
public class KStreamsController {

    private final RetailSalesApp retailSalesApp = new RetailSalesApp();

    public static void main(String[] args) {
        SpringApplication.run(KStreamsController.class, args);
    }

    @GetMapping("/start")
    public String startPipeline() throws Exception {
        generateData();
        retailSalesApp.startStreamProcessing();
        return "Kafka Streams pipeline started with sample data.";
    }

    private void generateData() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ObjectMapper mapper = new ObjectMapper();

        // Products
        producer.send(new ProducerRecord<>("products", "p1", mapper.writeValueAsString(new Product("p1", 100.0))));
        producer.send(new ProducerRecord<>("products", "p2", mapper.writeValueAsString(new Product("p2", 200.0))));
        producer.send(new ProducerRecord<>("products", "p3", mapper.writeValueAsString(new Product("p3", 300.0))));


        // Purchases
        Random rand = new Random();
        for (int i = 0; i < 10; i++) {
            Purchase purchase = new Purchase("c" + rand.nextInt(5), "p" + (1 + rand.nextInt(3)), 1 + rand.nextInt(3), System.currentTimeMillis());
            producer.send(new ProducerRecord<>("purchases", purchase.getProductId(), mapper.writeValueAsString(purchase)));
            Thread.sleep(500);
        }

        producer.flush();
        producer.close();
    }
}
