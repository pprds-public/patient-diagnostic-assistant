package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.javadsl.Source;

/**
 * @author Mohammed El Bahja
 */
public interface KafkaConsumerFactory {
  Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> dataTransporterStringConsumer(String topic);
  ConsumerSettings<String, String> consumerStrSettings();
  ConsumerSettings<String, byte[]> consumerBinarySettings();
}
