package com.pprdsystems.ai.agentic.prompts.chaining.workflow.normalizer;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public final class Helpers {
  private Helpers() { throw new RuntimeException("Helpers is an utility class and cannot be instantiated."); }

  /**
   *
   * @param var
   * @param dflt
   * @return
   */
  public static String env(String var, String dflt) {
    String value = System.getenv(var);
    return (value == null || value.isBlank()) ? dflt : value;
  }

  /**
   *
   * @param brokers
   * @param topic
   * @param partitions
   * @param rf
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public static void ensureTopic(String brokers, String topic, int partitions, short rf) throws ExecutionException, InterruptedException {
    try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", brokers))) {
      Set<String> names = admin.listTopics().names().get();

      if (!names.contains(topic)) {
        System.out.println("Creating topic: " + topic);
        CreateTopicsResult res = admin.createTopics(List.of(new NewTopic(topic, partitions, rf)));
        try { res.all().get(); }
        catch (ExecutionException ee) { if (!(ee.getCause() instanceof org.apache.kafka.common.errors.TopicExistsException)) throw ee; }
      }
    }
  }

  /**
   *
   * @param n
   * @param f
   * @return
   */
  public static String toText(JsonNode n, String f) {
    JsonNode v = (n == null) ? null : n.get(f);
    return (v == null || v.isNull()) ? null : v.asText();
  }

  /**
   *
   * @param s
   * @return
   */
  public static boolean isBlank(String s) { return s == null || s.trim().isEmpty(); }
  public static String esc(String s) {
    if (s == null) return "";
    return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
  }

  /**
   *
   * @param t
   * @return
   */
  public static String toMessage(Throwable t) {
    String m = (t == null) ? "" : Objects.toString(t.getMessage(), "");
    return m.length() > 300 ? m.substring(0, 300) : m;
  }

  /**
   *
   * @param raw
   * @return
   */
  public static String literalOrQuoted(String raw) {
    if (raw != null) {
      String t = raw.trim();
      if ((t.startsWith("{") && t.endsWith("}")) || (t.startsWith("[") && t.endsWith("]"))) return t;
    }
    return "\"" + esc(raw) + "\"";
  }
}
