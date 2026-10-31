package dev.dmitry.retailstream.support;

import com.github.dockerjava.api.model.*;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.testcontainers.kafka.KafkaContainer;

public final class TestBroker implements AutoCloseable {
  public final TestDatabase database = new TestDatabase();
  public final KafkaContainer container;

  public TestBroker() {
    container =
        new KafkaContainer(
                "apache/kafka@sha256:8f325fc00143fed78eabe00b033a86747078bb06edc21345d06651f77ae44f30")
            .withStartupTimeout(Duration.ofSeconds(30))
            .withEnv("KAFKA_HEAP_OPTS", "-Xms256m -Xmx512m")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0");
    container.withCreateContainerCmdModifier(
        cmd ->
            cmd.getHostConfig()
                .withMemory(1536L * 1024 * 1024)
                .withPortBindings(
                    new PortBinding(
                        Ports.Binding.bindIpAndPort("127.0.0.1", 0),
                        ExposedPort.tcp(container.getExposedPorts().getFirst()))));
    try {
      container.start();
      try (var admin =
          AdminClient.create(Map.of("bootstrap.servers", container.getBootstrapServers()))) {
        if (admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS) == null)
          throw new IllegalStateException("No actual broker identity");
      }
      System.out.println("ACTUAL_KAFKA_IMAGE=" + container.getDockerImageName());
    } catch (Exception ex) {
      close();
      throw new IllegalStateException("Broker startup failed", ex);
    }
  }

  @Override
  public void close() {
    container.close();
    database.close();
  }
}
