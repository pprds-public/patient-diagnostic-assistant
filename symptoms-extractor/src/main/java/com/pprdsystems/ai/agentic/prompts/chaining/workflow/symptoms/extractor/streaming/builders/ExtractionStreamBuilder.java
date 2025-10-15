package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.builders;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.SymptomsExtractor;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaComponentsFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.NormalizedNotesStream;

/**
 *
 * @param <T>
 *
 * @author Mohammed El Bahja
 */
public interface ExtractionStreamBuilder<T> {
  ExtractionStreamBuilder<T> appConfig(AppConfiguration appConfiguration);
  ExtractionStreamBuilder<T> name(String name);
  ExtractionStreamBuilder<T> symptomsExtractor(SymptomsExtractor symptomsExtractor);
  ExtractionStreamBuilder<T> kafkaComponentsFactory(KafkaComponentsFactory kafkaComponentsFactory);
  NormalizedNotesStream<T> build();
}
