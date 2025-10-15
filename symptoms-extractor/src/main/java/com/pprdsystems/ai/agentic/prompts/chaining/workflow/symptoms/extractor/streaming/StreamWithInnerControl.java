package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming;

import org.apache.pekko.kafka.javadsl.Consumer.Control;
import org.apache.pekko.stream.javadsl.RunnableGraph;

import java.util.concurrent.atomic.AtomicReference;

/**
 * @author Mohammed El Bahja
 */
public interface StreamWithInnerControl<T> {
  String getName();
  AtomicReference<Control> getInnerControl();
  RunnableGraph<StreamGraph<T>> createRunnableGraph();
}
