package dev.dmitry.retailstream.kafka;

import dev.dmitry.retailstream.projection.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.*;
import tools.jackson.databind.json.JsonMapper;

public final class KafkaRuntime implements AutoCloseable {
  private final String bootstrap;
  private final String fixtureSha256;
  private final ProjectionStore store;
  private final ProcessingHooks hooks;
  private final AdminClient admin;

  public KafkaRuntime(String bootstrap, String fixtureSha256) {
    this(bootstrap, fixtureSha256, null, ProcessingHooks.NONE);
  }

  public KafkaRuntime(
      String bootstrap, String fixtureSha256, ProjectionStore store, ProcessingHooks hooks) {
    this.bootstrap = bootstrap;
    this.fixtureSha256 = fixtureSha256;
    this.store = store;
    this.hooks = hooks;
    admin =
        AdminClient.create(
            Map.of(
                "bootstrap.servers",
                bootstrap,
                "request.timeout.ms",
                10000,
                "default.api.timeout.ms",
                30000));
  }

  public String bootstrap() {
    return bootstrap;
  }

  public String fixtureSha256() {
    return fixtureSha256;
  }

  public void createTopic(String topic) {
    if (!topic.matches("p22\\.[A-Za-z0-9._-]{1,244}"))
      throw new IllegalArgumentException("Unsafe topic");
    try {
      admin
          .createTopics(
              List.of(
                  new NewTopic(topic, 3, (short) 1)
                      .configs(Map.of("cleanup.policy", "delete", "retention.ms", "604800000"))))
          .all()
          .get(30, TimeUnit.SECONDS);
    } catch (Exception ex) {
      throw new IllegalStateException("Cannot create fresh topic", ex);
    }
  }

  public SourceBoundary boundary(String topic) {
    try {
      if (admin
              .describeTopics(List.of(topic))
              .allTopicNames()
              .get(5, TimeUnit.SECONDS)
              .get(topic)
              .partitions()
              .size()
          != 3) throw new IllegalStateException("Partition count changed");
      Map<TopicPartition, OffsetSpec> earliest = new HashMap<>(), latest = new HashMap<>();
      for (int p = 0; p < 3; p++) {
        var tp = new TopicPartition(topic, p);
        earliest.put(tp, OffsetSpec.earliest());
        latest.put(tp, OffsetSpec.latest());
      }
      var first = admin.listOffsets(earliest).all().get(5, TimeUnit.SECONDS);
      var last = admin.listOffsets(latest).all().get(5, TimeUnit.SECONDS);
      Map<Integer, Long> beginning = new TreeMap<>(), end = new TreeMap<>();
      for (int p = 0; p < 3; p++) {
        var tp = new TopicPartition(topic, p);
        beginning.put(p, first.get(tp).offset());
        end.put(p, last.get(tp).offset());
      }
      return new SourceBoundary(topic, beginning, end, fixtureSha256);
    } catch (Exception ex) {
      throw new IllegalStateException("Cannot observe source boundaries", ex);
    }
  }

  public Map<Integer, Long> committed(String groupId, String topic) {
    try {
      var offsets =
          admin
              .listConsumerGroupOffsets(groupId)
              .partitionsToOffsetAndMetadata()
              .get(5, TimeUnit.SECONDS);
      Map<Integer, Long> result = new TreeMap<>();
      for (int p = 0; p < 3; p++) {
        var offset = offsets.get(new TopicPartition(topic, p));
        result.put(p, offset == null ? null : offset.offset());
      }
      return Collections.unmodifiableMap(result);
    } catch (Exception ex) {
      throw new IllegalStateException("Cannot observe committed offsets", ex);
    }
  }

  Map<String, Object> consumerProperties(String groupId) {
    Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 120000);
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
    props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 1000);
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false);
    props.put("group.protocol", "classic");
    return props;
  }

  public void requireUnchanged(SourceBoundary expected) {
    if (!fixtureSha256.equals(expected.fixtureSha256())
        || !boundary(expected.topic()).equals(expected))
      throw new IllegalStateException("Source boundary or fixture changed");
  }

  public void consumeTo(ProjectionConfig config, SourceBoundary target) {
    if (store == null
        || !config.topic().equals(target.topic())
        || !config.fixtureSha256().equals(fixtureSha256))
      throw new IllegalArgumentException("Consumer/store source mismatch");
    requireUnchanged(target);
    var failure = new AtomicReference<RuntimeException>();
    var properties = new ContainerProperties(config.topic());
    properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    properties.setSyncCommits(true);
    properties.setSyncCommitTimeout(Duration.ofSeconds(5));
    properties.setStopImmediate(true);
    properties.setShutdownTimeout(5000);
    properties.setPollTimeout(100);
    properties.setMessageListener(new ProjectionListener(store, hooks, failure));
    var container =
        new KafkaMessageListenerContainer<byte[], byte[]>(
            new DefaultKafkaConsumerFactory<>(consumerProperties(config.groupId())), properties);
    container.setCommonErrorHandler(new CommonContainerStoppingErrorHandler());
    container.setBeanName("p22-" + config.schema());
    long startupDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    try {
      container.start();
      while (container.getAssignedPartitions() == null
          || container.getAssignedPartitions().size() != 3) {
        if (failure.get() != null) throw failure.get();
        if (System.nanoTime() > startupDeadline)
          throw new IllegalStateException("Consumer startup exceeded 30 seconds");
        Thread.sleep(50);
      }
      System.out.println(
          new JsonMapper()
              .writeValueAsString(
                  Map.of(
                      "phase",
                      "assignment",
                      "groupId",
                      config.groupId(),
                      "partitions",
                      container.getAssignedPartitions().stream()
                          .map(TopicPartition::partition)
                          .sorted()
                          .toList())));
      long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
      while (true) {
        if (failure.get() != null) throw failure.get();
        var offsets = committed(config.groupId(), config.topic());
        boolean reached = true;
        for (int p = 0; p < 3; p++) {
          Long observed = offsets.get(p);
          long effective = observed == null ? target.beginning().get(p) : observed;
          if (effective > target.end().get(p))
            throw new IllegalStateException("Group offset passed frozen end");
          if (effective < target.end().get(p)) reached = false;
        }
        if (reached) {
          requireUnchanged(target);
          return;
        }
        if (!container.isRunning())
          throw new IllegalStateException("Consumer stopped before catch-up");
        if (System.nanoTime() > deadline)
          throw new IllegalStateException("Catch-up exceeded 60 seconds");
        Thread.sleep(100);
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Consumer interrupted", ex);
    } finally {
      container.stop();
    }
  }

  @Override
  public void close() {
    admin.close(Duration.ofSeconds(5));
  }
}
