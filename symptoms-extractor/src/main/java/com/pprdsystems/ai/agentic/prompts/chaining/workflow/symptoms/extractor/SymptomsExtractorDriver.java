package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.DefaultKafkaComponentsFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaComponentsFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.NormalizedNotesStream;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.StreamGraph;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.pekko.Done;
import org.apache.pekko.actor.CoordinatedShutdown;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.stream.ActorAttributes;
import org.apache.pekko.stream.FlowMonitor;
import org.apache.pekko.stream.Supervision;
import org.apache.pekko.stream.javadsl.RunnableGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming.builders.DefaultExtractionStreamBuilder.extractionStreamBuilder;

/**
 * Symptoms Extractor Model
 * It consumes normalized note JSON then extracts symptoms, severity, and duration then publishes structured symptoms/history to symptoms.extracted.v1 using OpenAI.
 * This gives the orchestrator something concrete to call.
 *
 * @author Mohammed El Bahja
 */
public class SymptomsExtractorDriver {
  private static final Logger log = LoggerFactory.getLogger(SymptomsExtractorDriver.class.getName());

  private final Config config = ConfigFactory.load();
  private final AppConfiguration appConfiguration = new AppConfiguration(config);
  private final KafkaComponentsFactory kafkaComponentsFactory = new DefaultKafkaComponentsFactory(appConfiguration);
  private final ActorSystem<?> actorSystem;
  private final SymptomsExtractor symptomsExtractor;

  public SymptomsExtractorDriver() {
    this.actorSystem = ActorSystem.create(Behaviors.empty(), "pprd-systems");
    this.symptomsExtractor = new SymptomsExtractor();
  }

  /**
   *
   * @param args
   */
  public static void main(String[] args)  {
    SymptomsExtractorDriver application = new SymptomsExtractorDriver();
    application.start(args);
  }

  /**
   *
   * @param args
   */
  public void start(String[] args) {
    List<NormalizedNotesStream<CompletionStage<Done>>> normalizedNotesStreamsList = new ArrayList<>();

    NormalizedNotesStream<CompletionStage<Done>> normalizedNotesStream = extractionStreamBuilder()
        .appConfig(this.appConfiguration)
        .name("symptoms-extraction-stream")
        .symptomsExtractor(this.symptomsExtractor)
        .kafkaComponentsFactory(this.kafkaComponentsFactory)
        .build();

    normalizedNotesStreamsList.add(normalizedNotesStream);

    Map<String, FlowMonitor<?>> extractionStreamsMonitors = runExtractionStreams(normalizedNotesStreamsList);

    // extractionStreamsMonitors could be passed to a REST api to make the user able to request the state of the stream
    extractionStreamsMonitors.forEach((name, flowMonitor) -> {
      System.out.printf("Pekko stream: %s with state: %s", name, flowMonitor.state());
    });
  }

  /**
   *
   * @param normalizedNotesStreamsList
   * @return
   */
  private Map<String, FlowMonitor<?>> runExtractionStreams(List<NormalizedNotesStream<CompletionStage<Done>>> normalizedNotesStreamsList) {
    Map<String, NormalizedNotesStream<CompletionStage<Done>>> extractionStreams = normalizedNotesStreamsList.stream()
        .map(stream -> Map.entry(stream.getName(), stream))
        .collect(Collectors.toMap(Map.Entry::getKey,  Map.Entry::getValue));

    return extractionStreams.entrySet().stream()
      .map(entry -> {
        String name = entry.getKey();
        NormalizedNotesStream<CompletionStage<Done>> stream = entry.getValue();

        CoordinatedShutdown.get(actorSystem)
          .addTask(CoordinatedShutdown.PhaseActorSystemTerminate(), String.format("%s_shutdown", name), () -> {
            System.out.printf("Draining and shutting down Pekko stream: %s", name);

            return stream.getInnerControl()
                .get()
                .shutdown();
          });

        RunnableGraph<StreamGraph<CompletionStage<Done>>> runnableGraph = stream.createRunnableGraph()
          .withAttributes(ActorAttributes.withSupervisionStrategy(exc -> {
            log.error(ExceptionUtils.getStackTrace(exc));
            return Supervision.resume();
          }));

        System.out.printf("Starting Pekko Stream: %s%n", name);

        StreamGraph<CompletionStage<Done>> streamGraph = runnableGraph.run(actorSystem);

        streamGraph.result()
            .whenComplete((done, throwable) -> {
              if (throwable != null) {
                System.out.printf("An exception has occurred while running the stream %s%n", name);
              } else {
                System.out.printf("stream %s's graph completed.%n", name);
              }
            });

        FlowMonitor<?> monitor = streamGraph.monitor();

        return Map.entry(name, monitor);
      })
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }
}


