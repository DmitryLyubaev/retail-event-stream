package dev.dmitry.retailstream.kafka;

import dev.dmitry.retailstream.contract.RawRecord;
import dev.dmitry.retailstream.projection.*;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import tools.jackson.databind.json.JsonMapper;

public final class ProjectionListener implements AcknowledgingMessageListener<byte[], byte[]> {
  private final BoundedRetry retry;
  private final ProcessingHooks hooks;
  private final AtomicReference<RuntimeException> failure;

  public ProjectionListener(
      ProjectionStore store, ProcessingHooks hooks, AtomicReference<RuntimeException> failure) {
    this.retry = new BoundedRetry(store::process, Thread::sleep);
    this.hooks = hooks;
    this.failure = failure;
  }

  @Override
  public void onMessage(ConsumerRecord<byte[], byte[]> record, Acknowledgment acknowledgment) {
    var raw =
        new RawRecord(
            record.topic(), record.partition(), record.offset(), record.key(), record.value());
    try {
      var result = retry.execute(raw);
      hooks.afterCommit(raw);
      acknowledgment.acknowledge();
      System.out.println(
          new JsonMapper()
              .writeValueAsString(
                  Map.of(
                      "phase",
                      "processed",
                      "topic",
                      raw.topic(),
                      "partition",
                      raw.partition(),
                      "offset",
                      raw.offset(),
                      "disposition",
                      result.disposition().name(),
                      "redelivery",
                      result.redelivery())));
    } catch (RuntimeException ex) {
      failure.compareAndSet(null, ex);
      throw ex;
    }
  }
}
