package com.example.kafkademo;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Utility for wiping Kafka topics clean between demo runs.
 * Deletes each topic (if it exists) and recreates it empty.
 */
public final class TopicResetter {

    private TopicResetter() {}

    public static void resetTopics(String bootstrapServers, String... topics) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000);

        try (AdminClient admin = AdminClient.create(props)) {
            // 1) Delete any that currently exist
            Set<String> existing = admin.listTopics().names().get();
            List<String> toDelete = List.of(topics).stream()
                    .filter(existing::contains)
                    .toList();

            if (!toDelete.isEmpty()) {
                System.out.println("[TopicResetter] Deleting topics: " + toDelete);
                try {
                    admin.deleteTopics(toDelete).all().get();
                } catch (ExecutionException e) {
                    if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw e;
                    }
                }
                // Deletion is async on the broker — wait until they're truly gone
                waitUntilGone(admin, toDelete);
            }

            // 2) Recreate them empty (1 partition, 1 replica — fine for local demo)
            List<NewTopic> newTopics = List.of(topics).stream()
                    .map(name -> new NewTopic(name, 1, (short) 1))
                    .toList();

            try {
                admin.createTopics(newTopics).all().get();
                System.out.println("[TopicResetter] Recreated topics: " + List.of(topics));
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
                // Already recreated by something else — fine.
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while resetting topics", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Failed to reset topics", e);
        }
    }

    private static void waitUntilGone(AdminClient admin, List<String> topics)
            throws InterruptedException, ExecutionException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            Set<String> still = admin.listTopics().names().get();
            if (topics.stream().noneMatch(still::contains)) return;
            Thread.sleep(200);
        }
        System.err.println("[TopicResetter] Warning: deletion still propagating after 10s");
    }
}
