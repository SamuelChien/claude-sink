# claude-sink

CLI that reads skills, Claude sessions, and source code from your filesystem and writes them as consumption chunks to Kafka. Includes consumers that analyze each chunk and produce enriched output to downstream topics.

```
Files on disk → claude-sink produce → Kafka → claude-sink consume → Analyzed topics
```

## Quick Start (5 minutes)

### Prerequisites

- Node.js 18+
- Access to a Kafka cluster (local Docker or GKE)
- `kubectl` configured if using GKE

### 1. Clone and build

```bash
git clone https://github.com/SamuelChien/claude-sink.git
cd claude-sink
npm install
npm run build
```

### 2. Start Kafka

**Option A — GKE (existing cluster)**
```bash
# Deploy Kafka StatefulSet with persistent storage
kubectl apply -f infrastructure/k8s-kafka.yml

# Wait for kafka-0 pod to be 1/1 Running
kubectl get pods -n kafka -w

# Port-forward to your machine
kubectl port-forward -n kafka svc/kafka 9092:9092
```

**Option B — Local Docker**
```bash
docker compose -f infrastructure/docker-compose.yml up -d
```

### 3. Setup (provisions topics + hello-world test)

```bash
npx tsx src/index.ts setup
```

Expected output:
```
1. Testing Kafka connectivity...
   Connected to Kafka
2. Provisioning topics...
   Created topic: sink.skills (6 partitions, retention=7d)
   Created topic: sink.sessions (3 partitions, retention=7d)
   Created topic: sink.code (6 partitions, retention=7d)
   Created topic: sink.skills.analyzed (6 partitions, retention=30d)
   Created topic: sink.sessions.analyzed (3 partitions, retention=30d)
   Created topic: sink.code.analyzed (6 partitions, retention=30d)
   Created topic: sink.skills.dlq (1 partitions, retention=30d)
   Created topic: sink.sessions.dlq (1 partitions, retention=30d)
   Created topic: sink.code.dlq (1 partitions, retention=30d)
3. Verifying topics...
   All 9 topics exist
4. Running hello-world test...
   Produced test message
   Round-trip: OK
=== Setup complete ===
```

If you see `Cannot connect to Kafka`, check that port-forward is running or Docker is up.

### 4. Produce data

```bash
# Ingest skills (SKILL.md files)
npx tsx src/index.ts skills ~/dev/mega-skills-directory

# Ingest your Claude sessions
npx tsx src/index.ts sessions

# Ingest code from a project
npx tsx src/index.ts code ~/dev/my-project
```

Each command prints progress:
```
14:05:08 [INFO] Sent batch: 50 chunks (50/5269)
14:05:08 [INFO] Sent batch: 50 chunks (100/5269)
...
14:05:22 [INFO] Done: 5269 skill chunks sent
```

### 5. Consume and analyze

```bash
# Run a consumer (it processes messages and writes to .analyzed topic)
# Use Ctrl+C to stop — it saves progress and resumes from where it left off
npx tsx src/index.ts consume skills
npx tsx src/index.ts consume sessions
npx tsx src/index.ts consume code
```

Each consumer prints batch progress:
```
[skills-consumer] consuming sink.skills → sink.skills.analyzed (group: sink-skills-processor)
[skills-consumer] batch: 50 → sink.skills.analyzed (total: 50, failed: 0)
[skills-consumer] batch: 50 → sink.skills.analyzed (total: 100, failed: 0)
```

### 6. Verify

```bash
# Check topic offsets (run inside the kafka pod or via port-forward)
npx tsx scripts/hello-world-test.ts
```

---

## Dry Run (no Kafka needed)

Test the parsing and chunking without a Kafka cluster:

```bash
# See what skills would be sent
npx tsx src/index.ts skills ~/dev/mega-skills-directory --dry-run --limit 5

# See what sessions would be sent
npx tsx src/index.ts sessions --dry-run --limit 3

# See what code files would be sent
npx tsx src/index.ts code ~/dev/my-project --dry-run --limit 5
```

---

## Architecture

### Topics

| Topic | Partitions | Retention | Content |
|-------|-----------|-----------|---------|
| `sink.skills` | 6 | 7 days | Raw SKILL.md files |
| `sink.sessions` | 3 | 7 days | Raw Claude session JSONL |
| `sink.code` | 6 | 7 days | Raw source code files |
| `sink.skills.analyzed` | 6 | 30 days | Skills with categories, entities, quality scores |
| `sink.sessions.analyzed` | 3 | 30 days | Sessions with duration, tokens, tools, topics |
| `sink.code.analyzed` | 6 | 30 days | Code with line analysis, imports, complexity |
| `sink.*.dlq` | 1 | 30 days | Failed messages with error headers |

### Message Schema (ConsumptionChunk)

Every message across all topics uses the same envelope:

```typescript
{
  chunkId: string,        // SHA-256 hash
  sourceType: 'skill' | 'session' | 'code',
  sourceId: string,       // skill name, session UUID, or file path
  sourcePath: string,     // absolute path on disk
  timestamp: number,      // epoch ms
  content: string,        // JSON-serialized raw data
  contentLength: number,
  contentHash: string,    // SHA-256 for dedup
  metadata: { ... }       // type-specific (SkillMetadata | SessionMetadata | CodeMetadata)
}
```

### Consumer Analysis

**Skills** → categories (13 types), technologies, tools, platforms, concepts, quality score (0-100), complexity, keywords

**Sessions** → duration (quick/short/medium/long/marathon), token usage + cache hit rate, tool usage + files accessed, topic classification, commands, models used

**Code** → line counts (code/comment/blank), imports (external vs internal), cyclomatic complexity, function/class/export counts, file role (source/test/config/build/doc)

---

## CLI Reference

```
claude-sink <command> [options]

Commands:
  skills <dir>           Ingest SKILL.md files
  sessions [dir]         Ingest Claude sessions (default: ~/.claude)
  code <dir>             Ingest source code files
  all                    Run all three producers
  consume <type>         Consume and analyze (skills|sessions|code|all)
  setup                  Provision topics and verify connectivity

Global Options:
  -b, --brokers <hosts>  Kafka brokers (default: localhost:9092)
  --batch-size <n>       Messages per batch (default: 50)
  --dry-run              Parse without sending to Kafka
  --limit <n>            Max items to ingest (0 = all)
  -v, --verbose          Debug logging
  -q, --quiet            Errors only
```

---

## GKE Deployment

### Deploy everything

```bash
# 1. Kafka with persistent storage
kubectl apply -f infrastructure/k8s-kafka.yml

# 2. Always-on consumers
kubectl apply -f infrastructure/k8s-consumers.yml

# 3. Hourly producer CronJobs
kubectl apply -f infrastructure/k8s-producers.yml
```

### Build and push Docker image

```bash
gcloud builds submit --tag gcr.io/blobfish-ai-429200/claude-sink:latest .
```

### Check status

```bash
kubectl get pods -n kafka
kubectl get cronjobs -n kafka
kubectl logs -n kafka -l app=skills-consumer --tail=10
```

### Kafka UI

If deployed on GKE with LoadBalancer:
```bash
kubectl get svc kafka-ui -n kafka  # check EXTERNAL-IP
# Open http://<EXTERNAL-IP>:8080
```

---

## Project Structure

```
src/
  commands/         CLI command handlers (skills, sessions, code, consume, setup)
  readers/          File system readers (skills-reader, sessions-reader, code-reader)
  processing/       Analyzers (skills-analyzer, sessions-analyzer, code-analyzer)
  consumers/        Kafka consumer implementations
  chunking/         ConsumptionChunk creation
  kafka/            Producer, consumer, DLQ, admin, config
  types/            TypeScript interfaces
  utils/            Logger, hash, file filter, frontmatter parser
scripts/
  hello-world-test.ts    End-to-end round-trip test
  provision-topics.ts    Topic provisioning script
infrastructure/
  k8s-kafka.yml          Kafka StatefulSet + PVC + Kafka-UI
  k8s-consumers.yml      Consumer Deployments
  k8s-producers.yml      Producer CronJobs
  docker-compose.yml     Local Docker setup
```
