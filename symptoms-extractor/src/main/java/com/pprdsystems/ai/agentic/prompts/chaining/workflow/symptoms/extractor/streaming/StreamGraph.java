package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming;

import org.apache.pekko.japi.Pair;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.stream.FlowMonitor;

/**
 * @author Mohammed El Bahja
 */
public record StreamGraph<T>(FlowMonitor<?> monitor, T result) {

  public static <T> StreamGraph<T> apply(Pair<FlowMonitor<ProducerMessage.Envelope<String, String, ConsumerMessage.Committable>>, T> t) {
    return new StreamGraph<T>(t.first(), t.second());
  }

  public static <T> StreamGraph<T> apply2(Pair<FlowMonitor<ProducerMessage.Envelope<String, byte[], ConsumerMessage.Committable>>, T> t) {
    return new StreamGraph<T>(t.first(), t.second());
  }
}