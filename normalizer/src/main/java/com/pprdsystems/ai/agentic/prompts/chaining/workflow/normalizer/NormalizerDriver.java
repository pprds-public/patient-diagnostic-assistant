package com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer;

/**
 * Driver for the Normalizer to reads raw notes, cleans text, then publishes normalized data to patient_notes.normalized.v1.
 *
 * @author Mohammed El Bahja
 */
public class NormalizerDriver {
  public static void main(String[] args) throws Exception {
    NormalizerJob.main(args);
  }
}


