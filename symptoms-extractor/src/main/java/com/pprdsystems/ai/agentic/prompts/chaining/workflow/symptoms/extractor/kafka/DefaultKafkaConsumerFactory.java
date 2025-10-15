package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.KafkaConfiguration;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ConsumerSettings;
import org.apache.pekko.kafka.Subscriptions;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.javadsl.Source;

/**
 * @author Mohammed El Bahja
 */
public class DefaultKafkaConsumerFactory implements KafkaConsumerFactory {
  private final KafkaConfiguration kafkaConfiguration;

  public DefaultKafkaConsumerFactory(KafkaConfiguration kafkaConfiguration) {
    this.kafkaConfiguration = kafkaConfiguration;
  }

  @Override
  public ConsumerSettings<String, String> consumerStrSettings() {
    return ConsumerSettings.create(kafkaConfiguration.consumerConfig(), new StringDeserializer(), new StringDeserializer())
        .withBootstrapServers(kafkaConfiguration.bootstrapServers());
  }

  @Override
  public ConsumerSettings<String, byte[]> consumerBinarySettings() {
    return ConsumerSettings.create(kafkaConfiguration.consumerConfig(), new StringDeserializer(), new ByteArrayDeserializer())
        .withBootstrapServers(kafkaConfiguration.bootstrapServers());
  }

  @Override
  public Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> dataTransporterStringConsumer(String topic) {
    return org.apache.pekko.kafka.javadsl.Consumer.committableSource(this.consumerStrSettings(), Subscriptions.topics(topic));
  }
}