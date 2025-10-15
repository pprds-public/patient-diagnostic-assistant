package com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.streaming;

import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.AppConfiguration;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.SymptomsExtractor;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.configuration.PatientNotesTopics;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaComponentsFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaConsumerFactory;
import com.pprdsystems.ai.agentic.prompts.chaining.workflow.symptoms.extractor.kafka.KafkaProducerFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.pekko.Done;
import org.apache.pekko.NotUsed;
import org.apache.pekko.japi.Pair;
import org.apache.pekko.kafka.ConsumerMessage;
import org.apache.pekko.kafka.ProducerMessage;
import org.apache.pekko.kafka.javadsl.Consumer;
import org.apache.pekko.stream.RestartSettings;
import org.apache.pekko.stream.javadsl.*;

import java.time.Duration;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Stream normalizes patients notes from kafka then extract the symptoms using an LLM provider
 * then publish structured notes back into kafka for the diagnoser model
 *
 * @author Mohammed El Bahja
 */
public class DefaultNormalizedNotesStream implements NormalizedNotesStream<CompletionStage<Done>> {
  private final AtomicReference<Consumer.Control> innerControl = new AtomicReference<>(Consumer.createNoopControl());
  private final String name;
  private final PatientNotesTopics patientNotesTopics;
  private final KafkaProducerFactory kafkaProducerFactory;
  private final KafkaConsumerFactory kafkaConsumerFactory;
  private final SymptomsExtractor symptomsExtractor;

  public DefaultNormalizedNotesStream(AppConfiguration appConfiguration, String name, KafkaComponentsFactory kafkaComponentsFactory, SymptomsExtractor symptomsExtractor) {
    this.name = name;
    this.patientNotesTopics = appConfiguration.getPatientNotesTopics();
    this.kafkaProducerFactory = kafkaComponentsFactory.producerFactory();
    this.kafkaConsumerFactory = kafkaComponentsFactory.consumerFactory();
    this.symptomsExtractor = symptomsExtractor;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public AtomicReference<Consumer.Control> getInnerControl() {
    return innerControl;
  }

  /**
   *
   * @return
   */
  @Override
  public RunnableGraph<StreamGraph<CompletionStage<Done>>> createRunnableGraph() {
    Sink<ProducerMessage.Envelope<String, String, ConsumerMessage.Committable>, CompletionStage<Done>> committableProducer = kafkaProducerFactory.committableStringProducer(); //.dataTransporterCommittableProducer();

    return stream().monitorMat(Keep.right())
      .toMat(committableProducer, Keep.both())
      .mapMaterializedValue(StreamGraph::apply)
      .named(name);
  }

  /**
   *
   * @return
   */
  private Source<ProducerMessage.Envelope<String, String, ConsumerMessage.Committable>, NotUsed> stream() {
    return RestartSource.withBackoff(RestartSettings.create(Duration.ofSeconds(1), Duration.ofSeconds(2), 0.2), () -> {
      String patientNotesNormalizedTopic = this.patientNotesTopics.patientNotesNormalizedTopic();
      String symptomsExtractedTopic = this.patientNotesTopics.symptomsExtractedTopic();

      Sink<Pair<ProducerRecord<String, String>, ConsumerMessage.Committable>, CompletionStage<Done>> producer = createStringProducer();
      Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> consumer = kafkaConsumerFactory.dataTransporterStringConsumer(patientNotesNormalizedTopic);

      Source<Pair<ProducerRecord<String,String>, ConsumerMessage.Committable>, Consumer.Control> sourceWithContextc = toSourceWithContext(consumer, symptomsExtractedTopic)
        .alsoTo(producer);

      return sourceWithContextc.map(pair -> {
        ProducerRecord<String, String> record = pair.first();
        ConsumerMessage.Committable committableOffset = pair.second();

        return ProducerMessage.<String, String, ConsumerMessage.Committable>passThrough(committableOffset);
      });
    });
  }

  /**
   *
   * @param consumer
   * @param symptomsExtractedTopic
   * @return
   */
  private Source<Pair<ProducerRecord<String, String>, ConsumerMessage.Committable>, Consumer.Control> toSourceWithContext(Source<ConsumerMessage.CommittableMessage<String, String>, Consumer.Control> consumer, String symptomsExtractedTopic) {
    return consumer.flatMapConcat(message -> {
      // it receives normalized notes but still unstructured clinical text
      String normalizedNotes = message.record().value();

      // the unstructured normalized notes have to be lifted into structured clinical features that later models can reason about.
      String structuredClinicalFeatures = this.symptomsExtractor.extract(normalizedNotes);

      Source<ProducerRecord<String, String>, NotUsed> source1 = Source.single(structuredClinicalFeatures)
        .via(Flow.<String>create()
            .map(data -> new ProducerRecord<>(symptomsExtractedTopic, data)));

      return source1.asSourceWithContext(producerRecord -> message.committableOffset());
    })
    .map(tuple2 -> {
      var producerRecord = tuple2._1;
      var committableOffset = tuple2._2;
      return Pair.create(producerRecord, committableOffset);
    });
  }

  /**
   *
   * @return
   */
  private Sink<Pair<ProducerRecord<String, String>, ConsumerMessage.Committable>, CompletionStage<Done>> createStringProducer() {
    Sink<ProducerRecord<String, String>, CompletionStage<Done>> stringProducer = this.kafkaProducerFactory.stringProducer();

    FlowWithContext<ProducerRecord<String, String>, ConsumerMessage.Committable, ProducerRecord<String, String>, ConsumerMessage.Committable, NotUsed> flowWithContext = FlowWithContext.create();
    Flow<Pair<ProducerRecord<String, String>, ConsumerMessage.Committable>, Pair<ProducerRecord<String, String>, ConsumerMessage.Committable>, NotUsed> flowWhithoutContext = flowWithContext.asFlow();

    return flowWhithoutContext.map(Pair::first)
      .toMat(stringProducer, Keep.right());
  }
}
