# claude-sink

CLI that reads Claude sessions, skills, and source code from your filesystem and sinks them to Google Pub/Sub as consumption chunks. Includes consumers that analyze each chunk and produce enriched output.

```
Files on disk → claude-sink produce → Pub/Sub → claude-sink consume → Analyzed topics
```

**Cost: ~$0/month** (Pub/Sub is $0.04/million messages)

## Quick Start

```bash
git clone https://github.com/SamuelChien/claude-sink.git
cd claude-sink
npm install
npm run build

# Set your GCP project (one-time)
export SINK_PROJECT=your-gcp-project-id

# Setup (creates 9 topics + 3 subscriptions)
npx tsx src/index.ts setup --pubsub $SINK_PROJECT

# Produce your data
npx tsx src/index.ts sessions --pubsub $SINK_PROJECT     # Claude sessions from ~/.claude
npx tsx src/index.ts skills <dir> --pubsub $SINK_PROJECT  # SKILL.md files
npx tsx src/index.ts code <dir> --pubsub $SINK_PROJECT    # Source code files

# Consume and analyze
npx tsx src/index.ts consume skills --pubsub $SINK_PROJECT
```

## Commands

```
claude-sink sessions [dir]     Ingest Claude Code sessions (default: ~/.claude)
claude-sink skills <dir>       Ingest SKILL.md files
claude-sink code <dir>         Ingest source code files
claude-sink consume <type>     Consume and analyze (skills|sessions|code|all)
claude-sink setup              Provision topics and verify connectivity
claude-sink serve              Start HTTP ingestion server (optional)
```

### Options

| Flag | Description |
|------|-------------|
| `--pubsub <project>` | GCP project for Pub/Sub (or set `SINK_PROJECT` env var) |
| `--dry-run` | Parse and chunk without sending |
| `--force` | Skip dedup, re-produce all files |
| `--limit <n>` | Max items to ingest |
| `-s, --server <url>` | Send via HTTP server instead of direct Pub/Sub |

## Deduplication

Re-running a produce command skips unchanged files automatically. State is tracked in `~/.claude-sink/produced-hashes.json`. Use `--force` to re-produce everything.

```bash
npx tsx src/index.ts sessions --pubsub $SINK_PROJECT   # produces 219 sessions
npx tsx src/index.ts sessions --pubsub $SINK_PROJECT   # "Nothing new to produce."
npx tsx src/index.ts sessions --pubsub $SINK_PROJECT --force  # re-produces all 219
```

## Pub/Sub Topics

| Topic | Content |
|-------|---------|
| `sink-skills` | Raw SKILL.md files |
| `sink-sessions` | Raw Claude session JSONL |
| `sink-code` | Raw source code files |
| `sink-skills-analyzed` | Skills with categories, entities, quality scores |
| `sink-sessions-analyzed` | Sessions with duration, tokens, tools, topics |
| `sink-code-analyzed` | Code with line analysis, imports, complexity |
| `sink-*-dlq` | Failed messages |

## Consumer Analysis

**Skills** → categories (13 types), technologies, quality score (0-100), complexity, keywords

**Sessions** → duration bucket, token usage + cache hit rate, tool calls, topic classification, commands, models

**Code** → line counts, imports (external vs internal), cyclomatic complexity, function/class counts, file role

## Monitoring

Grafana dashboard on Cloud Run:
```
URL: https://grafana-pubsub-596260477175.us-central1.run.app
Login: admin / claudesink
```

Shows: publish rate, consume rate, backlog, DLQ counts, bytes throughput, oldest unacked message age.

## E2E Test

```bash
npx tsx scripts/e2e-test.ts $SINK_PROJECT
```

## Dry Run (no infra needed)

```bash
npx tsx src/index.ts sessions --dry-run --limit 5
npx tsx src/index.ts skills ~/dev/mega-skills-directory --dry-run --limit 5
npx tsx src/index.ts code ~/dev/my-project --dry-run --limit 5
```

## Kafka (alternative)

Kafka is still supported. Omit `--pubsub` and pass `--brokers`:

```bash
npx tsx src/index.ts sessions --brokers localhost:9092
npx tsx src/index.ts consume skills --brokers localhost:9092
```

## Project Structure

```
src/
  commands/        CLI handlers (skills, sessions, code, consume, setup, serve)
  readers/         File system readers
  processing/      Analyzers (skills, sessions, code)
  consumers/       Kafka consumer implementations
  pubsub/          Pub/Sub producer, consumer, setup
  chunking/        ConsumptionChunk creation + dedup
  kafka/           KafkaJS producer, consumer, DLQ, admin
  server/          HTTP ingestion server + client
  types/           TypeScript interfaces
  utils/           Logger, hash, state, file filter
```
