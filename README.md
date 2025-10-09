# Patient Diagnostic Assistance - Agentic AI System (WIP)
A modular and event-driven **Agentic AI system** using The Prompts Chaining Workflow. It extracts clinical insights from unstructured patient notes, generates interpretable diagnostic, and treatment recommendations. It demonstrates how structured orchestration, real-time pipelines, and LLM interactions can collaborate for interpretable healthcare decision-making.  

Each module runs independently and communicates through event-driven orchestration built on top of Apache Flink, Apache Pekko Streams, Kafka, and OpenAI Java SDK.

Each module simulates a cognitive layer:
- **Normalizer (Perception):** Prepares and standardizes data.
- **Extractor (Understanding):** Derives structured facts.
- **Diagnoser (Reasoning):** Infers likely diagnoses.
- **Planner (Action):** Suggests treatments or next steps.
- **Orchestrator (Memory & Control):** Coordinates and audits reasoning chain.

## Notes
> **Status:** Work in Progress - incremental updates. Some Updates might be published as LinkedIn articles.  
> **Vision:** A fully interpretable, event-driven and self-auditing AI workflow for healthcare diagnostics.  
> **Follow the journey:** [LinkedIn @PPRD Systems LLC](www.linkedin.com/in/melbahja) 

## 1. Purpose / Objective
### What problem are we solving?
Healthcare data, especially clinical notes, is mostly unstructured. This makes it difficult for AI systems to reason about or provide traceable insights.

### Why it matters
By creating an **Agentic AI workflow** that processes, interprets, and reasons about patient notes in modular stages, healthcare systems can achieve **interpretable diagnostics**, **automation**, and **decision support**.

### Who benefits
- **Clinicians & healthcare teams**: receive structured, traceable insights.
- **Data engineers & AI developers**: leverage modular, event-driven components.
- **Organizations**: enable auditable AI pipelines for compliance and analytics.

## 2. Context / Background
This system is part of PPRD Systems’ R&D on **Agentic AI Systems**. Intelligent and modular components that collaborate autonomously based on the article: [Summary of AI Systems’ Key Concepts](https://www.linkedin.com/pulse/summary-ai-systems-key-concepts-mohammed-el-bahja-oywee)

## 3. How It Works

### Step-by-Step
1. **Infrastructure**
   - (upcoming)
2. **Run Normalizer**
   - Reads raw notes, cleans text, publishes to `patient_notes.normalized.v1`.
3. **Run Extractor (LLM Model Interaction)**
   - Extracts symptoms, severity, and duration.
   - Publishes to `symptoms.extracted.v1`.
4. **Run Diagnoser (LLM Model Interaction)**
   - Consumes extracted features and infers possible diagnoses.
   - Publishes to `diagnoses.v1`.
5. **Run Planner (LLM Model Interaction)**
   - Suggests treatments based on diagnoses.
   - Publishes to `treatments.v1`.
6. **Run Pekko Orchestrator**
   - Coordinates LLM models chaining and event persistence.
   - Writes to PostgreSQL.
7. **Visualize results**
   - Superset dashboard connects to PostgreSQL to display results and diagnostics.

## 6. Tools & Technologies

| Category | Technology |
|-----------|-------------|
| Language | **Java 17 (Maven)** |
| Stream Processing | **Apache Flink 2.x** |
| Messaging | **Apache Kafka 3.7.0** |
| Orchestration | **Apache Pekko (Actors & Streams)** |
| Persistence | **PostgreSQL** |
| Visualization | **Apache Superset** |
| LLM Platforms | **OpenAI** |
| Build | Maven + Shade Plugin |
| Environment | Docker Compose (local development) |

## 7. Architecture / Design

### High-Level Workflow
(upcoming)

### Design Principles
- **Event-driven architecture:** Each model communicates through Kafka topics.  
- **Composable architecture:** Modules can be swapped, upgraded, or replaced.  
- **Local-first:** All modules run on a local stack without cloud dependencies.  
- **Auditable and interpretable:** Every stage logs its transformations for compliance.

## 8. Implementation Details

### Folder Structure
```
patient-diagnostic-assistant/
├─ normalizer/
│  └─ notes-normalizer/         # Apache Flink ingestion job
├─ services/
│  ├─ extractor/                # Symptom & feature extraction
│  ├─ diagnoser/                # Diagnosis inference (upcoming)
│  └─ planner/                  # Treatment planner (upcoming)
├─ orchestration/
│  └─ orchestrator/             # Actor-based workflow orchestration (upcoming)
├─ libs/
│  ├─ domain-model/              # DTOs, schemas (JSON/Avro/Proto)
│  └─ clients/                   # Shared Kafka/gRPC/HTTP clients (upcoming)
├─ infra/
│  ├─ docker/                    # Local environment (Kafka, PG, Superset) (upcoming)
│  └─ sql/                       # Iceberg/PostgreSQL schema setup (upcoming)
└─ scripts/
   ├─ make_topics.sh             # (upcoming)
   ├─ seed_notes.sh
   └─ run_local.sh
```

### Module Summary
| Module | Description |
|---------|--------------|
| **Normalizer** | Cleans incoming patient notes using Flink. |
| **Extractor** | Extracts structured symptoms, duration, severity, negations. |
| **Diagnoser** | Predicts likely diagnoses from Extractor output. |
| **Planner** | Suggests evidence-based treatment plans. |
| **Orchestrator** | Manages model chaining, event routing, and persistence. |
| **Superset Dashboards** | Visual layer for analytics and validation. |


## 9. Challenges & Solutions
| Challenge | Solution |
|------------|-----------|
| Flink 2.x API differences | Migrated to new `KafkaSource`/`KafkaSink` APIs. |
| Serialization consistency | Unified JSON schema in `libs/domain-model`. |
| Pipeline validation | Added Kafka peek jobs for traceability. |
| Throughput tuning | Used simple delivery guarantees for fast iteration. |


## 10. Testing / Validation
- **Integration testing:** (upcoming).  
- **Functional validation:** Extractor outputs matched expected patterns (upcoming).  
- **Performance checks:** Tested throughput with synthetic data (upcoming).  
- **Dataset:** Synthetic patient notes with varying severity, duration, and symptoms (upcoming).  
- **Tools:** Flink logs, Superset dashboards (upcoming).


## 11. Next Steps / TODOs

| Area | Task |
|-------|------|
| diagnoser | Implement diagnosis prediction (DL4J or TensorFlow). |
| planner | Add planner using rule-based + ML logic. |
| Orchestrator | Expand Pekko actor supervision & workflow routing. |
| Schema | Define Protobuf schemas and registry. |
| Visualization | Build Superset dashboards for full traceability. |
| Deployment | Docker Compose integration for full pipeline run. |

## 12. References
- [Apache Flink](https://flink.apache.org/)  
- [Apache Kafka](https://kafka.apache.org/)  
- [Apache Pekko](https://pekko.apache.org/)  
- [Apache Iceberg](https://iceberg.apache.org/)  
- [Apache Superset](https://superset.apache.org/)  
- [Deeplearning4j (DL4J)](https://deeplearning4j.konduit.ai/)
- [OpenAI API & SDKs](https://platform.openai.com/docs/quickstart/)
- LinkedIn Articles:  
  - [Summary of AI Systems’ Key Concepts](https://www.linkedin.com/pulse/summary-ai-systems-key-concepts-mohammed-el-bahja-oywee)
  - [Few ways to use AI across domains in practice](https://www.linkedin.com/pulse/few-ways-use-ai-across-domains-practice-mohammed-el-bahja-qpl5e)
