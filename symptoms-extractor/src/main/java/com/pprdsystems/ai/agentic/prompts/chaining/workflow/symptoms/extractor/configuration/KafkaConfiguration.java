package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration;

import com.typesafe.config.Config;

/**
 *
 * @param producerConfig
 * @param consumerConfig
 * @param committerConfig
 * @param bootstrapServers
 * @param dataBuffer
 *
 * @author Mohammed El Bahja
 */
public record KafkaConfiguration(
    Config producerConfig,
    Config consumerConfig,
    Config committerConfig,
    String bootstrapServers,
    int dataBuffer
) {}
