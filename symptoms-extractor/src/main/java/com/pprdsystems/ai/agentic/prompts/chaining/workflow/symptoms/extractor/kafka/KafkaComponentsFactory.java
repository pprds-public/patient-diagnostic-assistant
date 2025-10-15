package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.KafkaConfiguration;
import org.apache.pekko.kafka.*;

/**
 * Provides Kafka consumer or producer instances with different configuration
 *
 * @author Mohammed El Bahja
 */
public interface KafkaComponentsFactory {
  KafkaConfiguration kafkaConfig();
  KafkaProducerFactory producerFactory();
  KafkaConsumerFactory consumerFactory();
  CommitterSettings committerSettings();
}
