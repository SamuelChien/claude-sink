# claude-sink

CLI tool to ingest skills, Claude sessions, and source code into Kafka topics as consumption chunks.

## Install

```bash
npm install
npm run build
```

## Usage

```bash
# Ingest SKILL.md files
claude-sink skills <dir> [options]

# Ingest Claude Code sessions from ~/.claude
claude-sink sessions [dir] [options]

# Ingest source code files
claude-sink code <dir> [options]

# Run all three
claude-sink all [options]
```

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `-b, --brokers` | Kafka broker addresses | `localhost:9092` |
| `--batch-size` | Messages per Kafka batch | `50` |
| `--dry-run` | Parse and chunk without sending | `false` |
| `--limit` | Max items to ingest (0 = all) | `0` |
| `-v, --verbose` | Verbose logging | `false` |
| `-q, --quiet` | Errors only | `false` |

### Examples

```bash
# Preview what would be sent
claude-sink skills ~/dev/mega-skills-directory --dry-run --limit 10

# Ingest sessions from last week
claude-sink sessions --since 2025-01-01

# Ingest code, skip test fixtures
claude-sink code ~/dev/my-project --exclude fixtures mocks
```

## Kafka Topics

| Topic | Key | Content |
|-------|-----|---------|
| `sink.skills` | skill id | One message per SKILL.md |
| `sink.sessions` | session UUID | One message per session |
| `sink.code` | relative file path | One message per source file |

## Infrastructure

```bash
# Start Kafka + Zookeeper + Kafka UI
npm run infra:up

# Kafka UI at http://localhost:8080
# Stop
npm run infra:down
```

## Development

```bash
npm run dev -- skills ~/dev/skills-dir --dry-run
npm run build        # TypeScript compile
npm run typecheck    # Type check only
```
