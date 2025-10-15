package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration;

/**
 *
 * @param patientNotesTopic
 * @param patientNotesNormalizedTopic
 * @param symptomsExtractedTopic
 *
 * @author Mohammed El Bahja
 */
public record PatientNotesTopics(
String patientNotesTopic,
String patientNotesNormalizedTopic,
String symptomsExtractedTopic
) {}
