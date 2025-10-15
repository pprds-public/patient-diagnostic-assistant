package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.KafkaConfiguration;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.pekko.Done;
import org.apache.pekko.kafka.CommitterSettings;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.stream.javadsl.Sink;

import java.util.concurrent.CompletionStage;

/**
 * @author Mohammed El Bahja
 */
public class DefaultKafkaProducerFactory implements KafkaProducerFactory {
  private final KafkaConfiguration kafkaConfiguration;
  private final CommitterSettings committerSettings;

  public DefaultKafkaProducerFactory(KafkaConfiguration kafkaConfiguration, CommitterSettings committerSettings) {
    this.kafkaConfiguration = kafkaConfiguration;
    this.committerSettings = committerSettings;
  }

  @Override
  public ProducerSettings<String, String> producerStrSettings() {
    return ProducerSettings.create(kafkaConfiguration.producerConfig(), new StringSerializer(), new StringSerializer())
        .withBootstrapServers(kafkaConfiguration.bootstrapServers());
  }

  @Override
  public Sink<ProducerRecord<String, String>, CompletionStage<Done>> stringProducer() {
    return org.apache.pekko.kafka.javadsl.Producer.plainSink(this.producerStrSettings());
  }

  @Override
  public Sink<ProducerMessage.Envelope<String, String, ConsumerMessage.Committable>, CompletionStage<Done>> committableStringProducer() {
    return org.apache.pekko.kafka.javadsl.Producer.committableSink(producerStrSettings(), this.committerSettings);
  }
}