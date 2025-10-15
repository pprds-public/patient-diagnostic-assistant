package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration;

import com.typesafe.config.Config;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Represent the application configuration.
 * @author Mohammed El Bahja
 */
public class AppConfiguration {
  private final Config config;
  private final Config consumerKafkaClients;
  private final Map<String, String> streamsExtraParams;
  private final KafkaConfiguration kafkaConfiguration;
  private final CommonTopics commonTopics;
  private final PatientNotesTopics patientNotesTopics;

  public AppConfiguration(Config config) {
    this.config               = config;
    this.consumerKafkaClients = config.getConfig("pekko.kafka.consumer.kafka-clients");
    this.streamsExtraParams   =  this.consumerKafkaClients.entrySet().stream()
        .map(Map.Entry::getKey)
        .map(consumerKafkaClients::getString)
        .collect(Collectors.toMap(v1 -> v1, v2 -> v2));

    this.kafkaConfiguration = new KafkaConfiguration(
        config.getConfig("pekko.kafka.producer"),
        config.getConfig("pekko.kafka.consumer"),
        config.getConfig("pekko.kafka.committer"),
        config.getString("pekko.kafka.bootstrapServers"),
        config.getInt("pekko.kafka.producer.data-buffer")
    );

    this.commonTopics = new CommonTopics(
      config.getString("pekko.kafka.common-topics.dlq-topic"),
      config.getString("pekko.kafka.common-topics.errors-topic")
    );

    this.patientNotesTopics = new PatientNotesTopics(
      config.getString("pekko.kafka.patient-notes-topics.notes-topic-in"),
      config.getString("pekko.kafka.patient-notes-notes-topic-out"),
      config.getString("pekko.kafka.patient-notes-topics.symptoms-extracted-topic")
    );
  }

  public KafkaConfiguration getKafkaConfiguration() {
    return kafkaConfiguration;
  }
  public CommonTopics getCommonTopics() {
    return commonTopics;
  }
  public  PatientNotesTopics getPatientNotesTopics() { return patientNotesTopics; }
}
