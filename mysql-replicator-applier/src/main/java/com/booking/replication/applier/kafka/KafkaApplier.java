package com.booking.replication.applier.kafka;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.Partitioner;
import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.commons.map.MapFilter;
import com.booking.replication.commons.metrics.Metrics;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class KafkaApplier implements Applier {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public interface Configuration {
        String TOPIC = "kafka.topic";
        String PRODUCER_PREFIX = "kafka.producer.";
        String BASE_PATH = "events";
    }

    private final Map<Integer, Producer<byte[], byte[]>> producers;
    private final Map<String, Object> configuration;
    private final String topic;
    private final int totalPartitions;
    private final Partitioner partitioner;
    private final Metrics<?> metrics;
    private final String delayName;

    public KafkaApplier(Map<String, Object> configuration) {
        Object topic = configuration.get(Configuration.TOPIC);

        Objects.requireNonNull(topic, String.format("Configuration required: %s", Configuration.TOPIC));

        this.producers = new ConcurrentHashMap<>();
        this.configuration = new MapFilter(configuration).filter(Configuration.PRODUCER_PREFIX);
        this.topic = topic.toString();
        this.totalPartitions = this.getTotalPartitions();
        this.partitioner = Partitioner.build(configuration);
        this.metrics = Metrics.build(configuration);
        this.delayName = MetricRegistry.name(
                Configuration.BASE_PATH,
                this.getList(configuration.getOrDefault(Metrics.Configuration.PATH, "delay")).toArray(new String[0])
        );
    }

    private Producer<byte[], byte[]> getProducer() {
        return new KafkaProducer<>(this.configuration, new ByteArraySerializer(), new ByteArraySerializer());
    }

    private int getTotalPartitions() {
        try (Producer<byte[], byte[]> producer = this.getProducer()) {
            return producer.partitionsFor(this.topic).stream().mapToInt(PartitionInfo::partition).max().orElseThrow(() -> new InvalidPartitionsException("partitions not found")) + 1;
        }
    }

    private List<String> getList(Object object) {
        if (List.class.isInstance(object)) {
            return (List<String>) object;
        } else {
            return Collections.singletonList(object.toString());
        }
    }

    @Override
    public Boolean apply(Collection<AugmentedEvent> events) {
        try {
            for (AugmentedEvent event : events) {
                int partition = this.partitioner.apply(event, this.totalPartitions);

                this.producers.computeIfAbsent(
                        partition, key -> this.getProducer()
                ).send(new ProducerRecord<>(
                        this.topic,
                        partition,
                        event.getHeader().getTimestamp(),
                        KafkaApplier.MAPPER.writeValueAsBytes(event.getHeader()),
                        KafkaApplier.MAPPER.writeValueAsBytes(event.getData())
                ));
                this.metrics.getRegistry().histogram(this.delayName).update(System.currentTimeMillis() - event.getHeader().getTimestamp());
            }

            return true;
        } catch (JsonProcessingException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    @Override
    public boolean forceFlush() {
        return false;
    }

    @Override
    public void close() throws IOException {
        this.partitioner.close();
        this.producers.values().forEach(Producer::close);
        this.producers.clear();
    }
}
