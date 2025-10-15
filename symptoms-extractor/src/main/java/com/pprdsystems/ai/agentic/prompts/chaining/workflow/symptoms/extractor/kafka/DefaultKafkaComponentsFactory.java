package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import org.apache.pekko.kafka.*;

/**
 * @author Mohammed El Bahja
 */
public class DefaultKafkaComponentsFactory extends AbstractKafkaComponentsFactory {

  public DefaultKafkaComponentsFactory(AppConfiguration appConfiguration) {
    super(appConfiguration);
  }

  @Override
  public KafkaProducerFactory producerFactory() {
    return new DefaultKafkaProducerFactory(kafkaConfig(), committerSettings());
  }

  @Override
  public KafkaConsumerFactory consumerFactory() {
    return new DefaultKafkaConsumerFactory(kafkaConfig());
  }

  @Override
  public CommitterSettings committerSettings() {
    return CommitterSettings.apply(kafkaConfig().committerConfig());
  }
}
