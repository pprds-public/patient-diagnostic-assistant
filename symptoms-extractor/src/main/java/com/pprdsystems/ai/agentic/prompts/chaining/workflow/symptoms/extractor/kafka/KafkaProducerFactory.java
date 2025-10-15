package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pekko.Done;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.ProducerSettings;
import org.apache.pekko.stream.javadsl.Sink;

import java.util.concurrent.CompletionStage;

/**
 * @author Mohammed El Bahja
 */
public interface KafkaProducerFactory {
  ProducerSettings<String, String> producerStrSettings();
  Sink<ProducerRecord<String, String>, CompletionStage<Done>> stringProducer();
  Sink<ProducerMessage.Envelope<String, String, ConsumerMessage.Committable>, CompletionStage<Done>> committableStringProducer();
}
