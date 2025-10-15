package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.builders;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.SymptomsExtractor;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaComponentsFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.DefaultNormalizedNotesStream;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.NormalizedNotesStream;
import org.apache.pekko.Done;

import java.util.concurrent.CompletionStage;

/**
 *
 * @author Mohammed El Bahja
 */
public class DefaultExtractionStreamBuilder implements ExtractionStreamBuilder<CompletionStage<Done>> {
  private AppConfiguration appConfiguration;
  private String name;
  private SymptomsExtractor symptomsExtractor;
  private KafkaComponentsFactory kafkaComponentsFactory;

  private DefaultExtractionStreamBuilder() {}

  public static ExtractionStreamBuilder<CompletionStage<Done>> extractionStreamBuilder() {
    return new DefaultExtractionStreamBuilder();
  }

  /**
   * Sets the {@code appConfiguration} and returns a reference to this Builder enabling method chaining.
   *
   * @param appConfiguration the {@code appConfiguration} to set
   * @return a reference to this Builder
   */
  @Override
  public ExtractionStreamBuilder<CompletionStage<Done>> appConfig(AppConfiguration appConfiguration) {
    this.appConfiguration = appConfiguration;
    return this;
  }

  /**
   * Sets the {@code name} and returns a reference to this Builder enabling method chaining.
   *
   * @param name the {@code name} to set
   * @return a reference to this Builder
   */
  @Override
  public ExtractionStreamBuilder<CompletionStage<Done>> name(String name) {
    this.name = name;
    return this;
  }

  /**
   *
   * @param symptomsExtractor
   * @return
   */
  @Override
  public ExtractionStreamBuilder<CompletionStage<Done>> symptomsExtractor(SymptomsExtractor symptomsExtractor) {
    this.symptomsExtractor = symptomsExtractor;
    return this;
  }

  /**
   * Sets the {@code kafkaComponentsFactory} and returns a reference to this Builder enabling method chaining.
   *
   * @param kafkaComponentsFactory the {@code kafkaComponentsFactory} to set
   * @return a reference to this Builder
   */
  @Override
  public ExtractionStreamBuilder<CompletionStage<Done>> kafkaComponentsFactory(KafkaComponentsFactory kafkaComponentsFactory) {
    this.kafkaComponentsFactory = kafkaComponentsFactory;
    return this;
  }

  /**
   * Returns a {@code NormalizedNotesStream} built from the parameters previously set.
   *
   * @return a {@code NormalizedNotesStream} built with parameters of this {@code DefaultExtractionStreamBuilder}
   */
  @Override
  public NormalizedNotesStream<CompletionStage<Done>> build() {
    return new DefaultNormalizedNotesStream(this.appConfiguration, this.name, this.kafkaComponentsFactory, this.symptomsExtractor);
  }
}
