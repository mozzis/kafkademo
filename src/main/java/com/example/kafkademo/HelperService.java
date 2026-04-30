package com.example.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HelperService {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String QUERIES_TOPIC = "Queries";
    private static final String RESPONSES_TOPIC = "Responses";
    private static final String CLIENT_ID = "helper-service";

    private static ExecutorService executor;

    public static void main(String[] args) {
        executor = Executors.newSingleThreadExecutor();
        executor.submit(HelperService::run);

        Runtime.getRuntime().addShutdownHook(new Thread(HelperService::shutdown));
    }

    private static void shutdown() {
        System.out.println("[HelperService] Shutting down...");
        // Clear any leftover messages from previous runs
//        TopicResetter.resetTopics(BOOTSTRAP_SERVERS, RESPONSES_TOPIC);
        executor.shutdownNow();
    }

    private static void run() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "helper-service-group");
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-producer");
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaConsumer<String, MQuery> consumer = new KafkaConsumer<>(
                    consumerProps, new StringDeserializer(), JsonSerDeser.deserializer(MQuery.class));
             KafkaProducer<String, MResponse> producer = new KafkaProducer<>(
                    producerProps, new StringSerializer(), JsonSerDeser.<MResponse>serializer())) {

            consumer.subscribe(Collections.singletonList(QUERIES_TOPIC));
            System.out.println("[HelperService] Listening on topic '" + QUERIES_TOPIC + "'...");

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, MQuery> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, MQuery> rec : records) {
                    MQuery query = rec.value();
                    if (query == null) continue;

                    // 1. Print the incoming query
                    System.out.printf(
                            "[HelperService] Received query %d: %s%.2f + %.2f%n",
                            query.getmSerial(), query.getmText(), query.getAddend1(), query.getAddend2());

                    // 2. Build a response
                    float sum = query.getAddend1() + query.getAddend2();
                    MResponse response = new MResponse("The result is: ", query.getmSerial(), sum);

                    // 3. Send the response back
                    producer.send(new ProducerRecord<>(RESPONSES_TOPIC, response),
                            (metadata, exception) -> {
                                if (exception != null) {
                                    System.err.println(
                                            "[HelperService] Failed to send response: " + exception.getMessage());
                                } else {
                                    System.out.printf(
                                            "[HelperService] Sent response: %s%.2f%n",
                                            response.getmText(), response.getSum());
                                }
                            });
                }
            }
        }
    }
}
