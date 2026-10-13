# ADR 0215: A topic's Avro schema belongs to its producer, and consumers only look it up

- Status: Accepted
- Date: 2026-10-14

## Context

Intelligence consumes Foundation's knowledge-source events. The value schema of that topic was a
string inside the consumer (`foundation_knowledge_avro.rs`, namespace
`kr.perfectory.foundation.events.fixture`, doc "Fixture-only"), and the consumer itself
registered it under the source subject and set the subject's compatibility. So the consumer
defined the producer's wire format: when Foundation writes a producer, the two copies can differ,
and whichever side registers first decides the contract. The only gate was an env flag
`FOUNDATION_KNOWLEDGE_CONSUMER_ENABLE_FIXTURE_CONTRACT`, and the default topic was an
Intelligence-named fixture topic.

Foundation already keeps its one published schema as a file named after its topic
(`platforms/foundation-platform/schemas/foundation-platform.catalog.collection-raw-written.v1.avsc`)
and embeds it with `include_str!`.

## Decision

1. The value schema of `foundation-platform.knowledge.source-upserted.v1` is the Foundation file
   `platforms/foundation-platform/schemas/<topic>.avsc`. The field set is unchanged; the record is
   `kr.perfectory.foundation.knowledge.KnowledgeSourceUpsertedV1`.
2. Intelligence embeds that file (`FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_SCHEMA`) and has no copy.
   The topic constant is `FOUNDATION_KNOWLEDGE_SOURCE_UPSERTED_TOPIC`.
3. The consumer never registers the source subject and never sets its compatibility. It looks
   the schema up (`POST /subjects/<topic>-value`, `KarapaceClient::lookup_avro_schema`); a
   subject Foundation has not registered stops start-up. This replaces the env flag, which is
   removed. Registering and BACKWARD_TRANSITIVE compatibility are the producer's job.
4. `messaging-infrastructure/tests/foundation_knowledge_schema_contract.rs` fails when the topic
   stops naming the file or when the codec stops writing and reading the file's schema (a
   renamed, retyped or new required field). The file is an input of `intelligence-ci.yml`
   (ADR-0202 §6).

## Consequences

- A Foundation change to the schema runs Intelligence CI, and a drift fails there before merge.
- The consumer cannot run until Foundation has a producer that registers the subject. That is
  the honest state today: there is no producer.
- Rejected: keeping the consumer-side registration with the Foundation file. It is the same
  file, but it still lets a consumer create the producer's subject and choose its compatibility.
- Rejected: a shared Rust crate. Cargo path dependencies across areas are forbidden (ADR-0001).
