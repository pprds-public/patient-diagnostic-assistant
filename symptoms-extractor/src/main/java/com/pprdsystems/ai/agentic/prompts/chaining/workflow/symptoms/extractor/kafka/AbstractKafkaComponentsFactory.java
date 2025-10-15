package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.KafkaConfiguration;

/**
 * @author Mohammed El Bahja
 */
public abstract class AbstractKafkaComponentsFactory implements KafkaComponentsFactory {
  protected final AppConfiguration appConfiguration;

  public AbstractKafkaComponentsFactory(AppConfiguration appConfiguration) {
    this.appConfiguration = appConfiguration;
  }

  @Override
  public KafkaConfiguration kafkaConfig() {
    return this.appConfiguration.getKafkaConfiguration();
  }
}
