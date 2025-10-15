package com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;

import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer.Constants.*;
import static com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer.Helpers.*;

/**
 * Flink Notes Normalizer (Ingestion & Pre-processing)
 * Input: raw data from Kafka topic: patient_notes.v1.
 * Output: cleaned, structured messages to Kafka topic: patient_notes.normalized.v1.
 * Every downstream component (orchestrator, models) expects normalized inputs.
 * Deliverables in this module:
 *    Flink job (NormalizerJob.java) -> Kafka source -> simple enrich/clean -> Kafka sink or DLQ.
 *
 * @author Mohammed El Bahja
 */
public final class NormalizerJob {
  private static final OutputTag<String> DLQ_TAG = new OutputTag<>("dlq", TypeInformation.of(String.class));

  public static void main(String[] args) throws Exception {
    // Ensure output topics exist
    ensureTopic(KAFKA_BROKER, NOTES_TOPIC_OUT, 1, (short)1);
    ensureTopic(KAFKA_BROKER, NOTES_TOPIC_DLQ, 1, (short)1);

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final Configuration config = new Configuration();

    config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
    config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///tmp/flink-checkpoints");

    env.configure(config);
    env.enableCheckpointing(1_000L, CheckpointingMode.EXACTLY_ONCE); // or CheckpointingMode.AT_LEAST_ONCE
    env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500L);
    env.getCheckpointConfig().setCheckpointTimeout(6_000L);
    env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
    // env.setParallelism(1); // while debugging

    // Flink only commits offsets on successful checkpoints. No checkpoints -> no commits.
    final KafkaSource<String> source = KafkaSource.<String>builder()
        .setBootstrapServers(KAFKA_BROKER)
        .setTopics(NOTES_TOPIC_IN)
        .setGroupId(NORMALIZER_JOB_GROUP_ID)
        .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
        .setValueOnlyDeserializer(new SimpleStringSchema()) // Deserialize message values as Strings
        // Let Flink manage commits on checkpoint; don't auto-commit in Kafka client
        .setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
        .build();

    final DataStream<String> raw = env.fromSource(
      source,
      WatermarkStrategy.noWatermarks(),
      "kafka-source-inTopic"
    );

    final SingleOutputStreamOperator<String> normalized = raw.process(new NormalizeFn(DLQ_TAG));
    final DataStream<String> dlq = normalized.getSideOutput(DLQ_TAG).map(v -> v);

    normalized.sinkTo(new KafkaProducerSink(KAFKA_BROKER, NOTES_TOPIC_OUT)).name("normalized-sink");
    dlq.sinkTo(new KafkaProducerSink(KAFKA_BROKER, NOTES_TOPIC_DLQ)).name("dlq-sink");

    env.execute("normalizer-job");
  }

  /**
   * Normalizer with DLQ
   */
  static final class NormalizeFn extends ProcessFunction<String, String> {
    private final OutputTag<String> dlqTag;
    private transient ObjectMapper mapper;

    private transient Counter inCounter, okCounter, dlqCounter, parseErrCounter;

    NormalizeFn(OutputTag<String> dlqTag) {
      this.dlqTag = dlqTag;
    }

    @Override
    public void open(OpenContext openContex) {
      this.mapper = new ObjectMapper();
      var g = getRuntimeContext().getMetricGroup();
      this.inCounter = g.counter("records_in");
      this.okCounter = g.counter("normalized_ok");
      this.dlqCounter = g.counter("to_dlq");
      this.parseErrCounter = g.counter("json_errors");
    }

    @Override
    public void processElement(String value, Context ctx, org.apache.flink.util.Collector<String> out) {
      System.out.println("NormalizeFn processElement: " + value);
      try {
        inCounter.inc();

        JsonNode n = mapper.readTree(value);
        String tenantId    = toText(n, "tenant_id");
        String encounterId = toText(n, "encounter_id");
        String noteText    = toText(n, "note_text");
        String ts          = toText(n, "ts");

        if (isBlank(tenantId) || isBlank(encounterId) || isBlank(noteText)) {
          String dlqJson = "{\"reason\":\"missing required fields\",\"payload\":" + literalOrQuoted(value) + "}";
          ctx.output(dlqTag, dlqJson);
          dlqCounter.inc();
          return;
        }

        String normalizedNote = noteText.trim().replaceAll("\\s+", " ");
        int noteLen = normalizedNote.length();

        String patientId   = toText(n, "patient_id_hash");
        String clinicianId = toText(n, "clinician_id_hash");

        StringBuilder sb = new StringBuilder(256)
            .append('{')
            .append("\"tenant_id\":\"").append(esc(tenantId)).append("\",")
            .append("\"encounter_id\":\"").append(esc(encounterId)).append("\",");

        if (!isBlank(patientId))   sb.append("\"patient_id_hash\":\"").append(esc(patientId)).append("\",");
        if (!isBlank(clinicianId)) sb.append("\"clinician_id_hash\":\"").append(esc(clinicianId)).append("\",");
        if (!isBlank(ts))          sb.append("\"ts\":\"").append(esc(ts)).append("\",");

        sb.append("\"note_text\":\"").append(esc(normalizedNote)).append("\",")
          .append("\"note_len\":").append(noteLen)
          .append('}');

        out.collect(sb.toString());
        okCounter.inc();

      } catch (Exception ex) {
        parseErrCounter.inc();
        System.out.println("NormalizeFn processElement: an exception has occured - " + ex.getMessage());
        String dlqJson = "{\"reason\":\"parse_or_normalize_error\","
            + "\"error\":\"" + esc(ex.getClass().getSimpleName()) + "\","
            + "\"message\":\"" + esc(toMessage(ex)) + "\","
            + "\"payload\":" + literalOrQuoted(value) + "}";
        ctx.output(dlqTag, dlqJson);
        dlqCounter.inc();
      }
    }
  }

  /**
   *
   */
  static final class KafkaProducerSink implements Sink<String> {
    private final String brokers;
    private final String topic;
    private final Properties extraProps; // optional

    KafkaProducerSink(String brokers, String topic) {
      this(brokers, topic, null);
    }

    KafkaProducerSink(String brokers, String topic, Properties extraProps) {
      this.brokers = brokers;
      this.topic = topic;
      this.extraProps = extraProps;
    }

    @Override
    public SinkWriter<String> createWriter(WriterInitContext context) throws IOException {
      return new Writer(context, brokers, topic, extraProps);
    }

    /**
     * Writer implementation (one per subtask)
     */
    static final class Writer implements SinkWriter<String> {
      private final String topic;
      private final Producer<String, String> producer;

      private final Counter inCounter;
      private final Counter okCounter;
      private final Counter errCounter;

      Writer(WriterInitContext ctx, String brokers, String topic, Properties extraProps) {
        this.topic = topic;

        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "1");
        p.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        p.put(ProducerConfig.RETRIES_CONFIG, "0");
        p.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        if (extraProps != null) p.putAll(extraProps);

        this.producer = new KafkaProducer<>(p);

        var mg = ctx.metricGroup();
        this.inCounter = mg.counter("sink_in");
        this.okCounter = mg.counter("sink_ok");
        this.errCounter = mg.counter("sink_err");
      }

      @Override
      public void write(String element, Context context) throws IOException, InterruptedException {
        inCounter.inc();
        if (element == null) {
          // Either drop or fail fast; here we fail to keep semantics clear:
          errCounter.inc();
          throw new IOException("KafkaProducerSinkV2 received null element");
        }
        try {
          // Block to surface errors immediately (simple at-least-once semantics)
          producer.send(new ProducerRecord<>(topic, null, element)).get();
          okCounter.inc();
        } catch (Exception e) {
          errCounter.inc();
          throw new IOException("Kafka send failed", e);
        }
      }

      @Override
      public void flush(boolean endOfInput) {
        try { producer.flush(); } catch (Exception ignore) {}
      }

      @Override
      public void close() {
        try { producer.flush(); } catch (Exception ignore) {}
        try { producer.close(); } catch (Exception ignore) {}
      }
    }
  }
}
