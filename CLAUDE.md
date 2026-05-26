# claude-sink

CLI tool that ingests skills, Claude sessions, and code into Kafka topics as consumption chunks.

## Quick Start

```bash
# Install deps
npm install

# Build
npm run build

# Dry-run to test without Kafka
npx tsx src/index.ts skills ~/dev/mega-skills-directory --dry-run --limit 10
npx tsx src/index.ts sessions --dry-run --limit 5
npx tsx src/index.ts code ~/dev/my-project --dry-run --limit 10

# Start Kafka
npm run infra:up

# Ingest for real
npx tsx src/index.ts skills ~/dev/mega-skills-directory
npx tsx src/index.ts sessions
npx tsx src/index.ts code ~/dev/my-project
```

## Kafka Topics

- `sink.skills` — One message per SKILL.md file
- `sink.sessions` — One message per Claude session
- `sink.code` — One message per source code file

## Project Structure

- `src/readers/` — File system readers for each source type
- `src/chunking/` — Converts reader output into ConsumptionChunk messages
- `src/kafka/` — KafkaJS producer (batched, idempotent)
- `src/commands/` — CLI subcommand handlers
- `infrastructure/` — Docker Compose for Kafka + Zookeeper + Kafka-UI
