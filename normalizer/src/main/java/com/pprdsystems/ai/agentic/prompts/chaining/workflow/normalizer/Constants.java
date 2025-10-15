package com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer;

import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer.Helpers.env;

public class Constants {

  private Constants() { throw new RuntimeException("Constants is an utility class and cannot be instantiated."); }

  public static final String KAFKA_BROKER = "localhost:9092";
  public static final String NOTES_TOPIC_IN = "patient_notes.v1";
  public static final String NOTES_TOPIC_OUT = "patient_notes.normalized.v1";
  public static final String NOTES_TOPIC_DLQ = "kafka.dlq.notes";
  public static final String NORMALIZER_JOB_GROUP_ID = env("GROUP_ID", "normalizer-job-group");
}
