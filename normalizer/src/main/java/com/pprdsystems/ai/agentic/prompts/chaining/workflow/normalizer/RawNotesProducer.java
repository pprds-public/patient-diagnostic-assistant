package com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer.Constants.KAFKA_BROKER;
import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer.Constants.NOTES_TOPIC_IN;

/**
 * Helper class to simulate publishing patients notes. It reads a JSONL file and publishes each line to Kafka topic patient_notes.v1 (or env override).
 *
 * Env - optional:
 *  - KAFKA_BROKER=localhost:9092
 *  - PATIENT_NOTES_KAFKA_TOPIC=patient_notes.v1
 *
 * Usage:
 *   java -jar <name>.jar produce data/sample_notes.jsonl
 */
public class RawNotesProducer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception {
    // TODO: extract the path as argument instead of hardcoding it
    String notesLocation = "./etc/data/patient_notes";

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.put("security.protocol", "PLAINTEXT");
    props.put("request.timeout.ms", "30000");
    props.put("metadata.max.age.ms", "5000");
    props.put("max.block.ms", "60000");
    props.put("reconnect.backoff.ms", "500");
    props.put("reconnect.backoff.max.ms", "5000");
    props.put("retry.backoff.ms", "500");


    String rawNotes = notesLocation + "/sample-notes.jsonl";
    String badNotes = notesLocation + "/bad-notes.jsonl";
    String invalidJson = notesLocation + "/invalid-json.json";

    publish(props, rawNotes);
    publish(props, badNotes);
    publish(props, invalidJson);
  }

  private static void publish(Properties props, String file)  throws Exception {
    System.out.printf("file: %s%nprops: %s%n", file, props);

    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props); BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      int count = 0;
      while ((line = br.readLine()) != null) {
        String key = extractEncounterId(line);
        ProducerRecord<String, String> rec = new ProducerRecord<>(NOTES_TOPIC_IN, key, line);
        producer.send(rec).get();
        System.out.println("sent " + rec.value() + " with key " + rec.key() + "to topic: " + rec.topic());
        count++;
        Thread.sleep(1000);
      }
      producer.flush();
      System.out.printf("Sent %d records to %s%n", count, NOTES_TOPIC_IN);
    }
  }

  private static String extractEncounterId(String jsonLine) {
    try {
      JsonNode n = MAPPER.readTree(jsonLine);
      JsonNode enc = n.get("encounter_id");
      return enc == null || enc.isNull() ? null : enc.asText();
    } catch (Exception e) {
      return null;
    }
  }
}
