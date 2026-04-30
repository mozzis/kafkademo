// demonstrate kafka producer/consumer operation.
// "CombatSystem" sends a series of addition problems for the helper service to solve
// When it receives the answers, it displays them
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CombatSystem {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String QUERIES_TOPIC = "Queries";
    private static final String RESPONSES_TOPIC = "Responses";
    private static final String CLIENT_ID = "combat-system";

    private static ExecutorService executor;
    // create a signal to wait for the consumer to be ready
    private static final CountDownLatch consumerReady = new CountDownLatch(1);

    public static void main(String[] args) {

        // one thread each fpr the producer and consumer
        executor = Executors.newFixedThreadPool(2);

        // Start the response listener first
        executor.submit(CombatSystem::consumeResponses);

        // Produce queries only after the consumer is ready
        executor.submit(CombatSystem::produceQueries);

        // arrange for shutdown to cleanup when done
        Runtime.getRuntime().addShutdownHook(new Thread(CombatSystem::shutdown));
    }

    private static void shutdown() {
        System.out.println("[CombatSystem] Shutting down...");
        // Clear any leftover messages from previous runs
//        TopicResetter.resetTopics(BOOTSTRAP_SERVERS, QUERIES_TOPIC);
        executor.shutdownNow();
    }

    private static void produceQueries() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Random random = new Random();

        try (KafkaProducer<String, MQuery> producer =
                     new KafkaProducer<>(props, new StringSerializer(), JsonSerDeser.serializer())) {

            // Wait until the response consumer has been assigned partitions
            try {
                consumerReady.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            while (!Thread.currentThread().isInterrupted()) {
                float addend1 = 1 + random.nextFloat() * 99;  // [1, 100)
                float addend2 = 1 + random.nextFloat() * 99;
                MQuery query = new MQuery("Please add these numbers: ", addend1, addend2);

                ProducerRecord<String, MQuery> record =
                        new ProducerRecord<>(QUERIES_TOPIC, query);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("[CombatSystem] Failed to send query: " + exception.getMessage());
                    } else {
                        System.out.printf(
                                "[CombatSystem] Sent query %d: %s%.2f + %.2f%n",
                                query.getmSerial(), query.getmText(), query.getAddend1(), query.getAddend2());
                    }
                });

                long sleepMs = 1000 + random.nextInt(2001); // 1000..3000 ms
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    private static void consumeResponses() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "combat-system-response-group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, CLIENT_ID + "-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        try (KafkaConsumer<String, MResponse> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), JsonSerDeser.deserializer(MResponse.class))) {

            consumer.subscribe(Collections.singletonList(RESPONSES_TOPIC),
                    new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
                        @Override
                        public void onPartitionsRevoked(
                                java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {}

                        @Override
                        public void onPartitionsAssigned(
                                java.util.Collection<org.apache.kafka.common.TopicPartition> partitions) {
                            System.out.println("[CombatSystem] Consumer assigned: " + partitions);
                            // don't want producer to start before consumer is ready
                            consumerReady.countDown(); // signal that consumer is ready
                        }
                    });

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, MResponse> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, MResponse> rec : records) {
                    MResponse response = rec.value();

                    if (response != null) {
                        System.out.printf(
                                "[CombatSystem] Received response %d: %s%.2f%n",
                                response.getmSerial(), response.getmText(), response.getSum());
                    }
                }
            }
        }
    }
}
