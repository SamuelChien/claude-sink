import { Command } from 'commander';
import { runSkills } from './commands/skills';
import { runSessions } from './commands/sessions';
import { runCode } from './commands/code';
import { runAll } from './commands/all';
import { runConsume } from './commands/consume';
import { runSetup } from './commands/setup';
import { startServer } from './server/index';
import { setLogLevel } from './utils/logger';

export function createCli(): Command {
  const program = new Command();

  program
    .name('claude-sink')
    .description('Ingest skills, Claude sessions, and code into Kafka topics')
    .version('0.1.0');

  program
    .command('skills')
    .description('Ingest SKILL.md files from a directory')
    .argument('<dir>', 'Skills directory to scan')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('-s, --server <url>', 'Send via HTTP server instead of direct Kafka')
    .option('--topic <name>', 'Kafka topic', 'sink.skills')
    .option('--batch-size <n>', 'Messages per batch', '50')
    .option('--dry-run', 'Parse and chunk without sending to Kafka', false)
    .option('--max-depth <n>', 'Max directory depth for recursive search', '5')
    .option('--limit <n>', 'Max number of skills to ingest (0 = all)', '0')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (dir: string, opts) => {
      applyLogLevel(opts);
      await runSkills(dir, {
        brokers: opts.brokers,
        server: opts.server,
        topic: opts.topic,
        batchSize: parseInt(opts.batchSize, 10),
        dryRun: opts.dryRun,
        maxDepth: parseInt(opts.maxDepth, 10),
        limit: parseInt(opts.limit, 10),
      });
    });

  program
    .command('sessions')
    .description('Ingest Claude sessions from ~/.claude (or specified dir)')
    .argument('[dir]', 'Claude directory (default: ~/.claude)')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('-s, --server <url>', 'Send via HTTP server instead of direct Kafka')
    .option('--topic <name>', 'Kafka topic', 'sink.sessions')
    .option('--batch-size <n>', 'Messages per batch', '50')
    .option('--dry-run', 'Parse and chunk without sending to Kafka', false)
    .option('--limit <n>', 'Max sessions to ingest (0 = all)', '0')
    .option('--since <date>', 'Only sessions after this ISO date')
    .option('--project <name>', 'Filter to specific project')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (dir: string | undefined, opts) => {
      applyLogLevel(opts);
      await runSessions(dir, {
        brokers: opts.brokers,
        server: opts.server,
        topic: opts.topic,
        batchSize: parseInt(opts.batchSize, 10),
        dryRun: opts.dryRun,
        limit: parseInt(opts.limit, 10),
        since: opts.since,
        project: opts.project,
      });
    });

  program
    .command('code')
    .description('Ingest source code files from a project directory')
    .argument('<dir>', 'Project directory to scan')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('-s, --server <url>', 'Send via HTTP server instead of direct Kafka')
    .option('--topic <name>', 'Kafka topic', 'sink.code')
    .option('--batch-size <n>', 'Messages per batch', '50')
    .option('--dry-run', 'Parse and chunk without sending to Kafka', false)
    .option('--exclude <dirs...>', 'Additional directory names to exclude')
    .option('--max-file-size <bytes>', 'Skip files larger than this', '1048576')
    .option('--limit <n>', 'Max files to ingest (0 = all)', '0')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (dir: string, opts) => {
      applyLogLevel(opts);
      await runCode(dir, {
        brokers: opts.brokers,
        server: opts.server,
        topic: opts.topic,
        batchSize: parseInt(opts.batchSize, 10),
        dryRun: opts.dryRun,
        exclude: opts.exclude,
        maxFileSize: parseInt(opts.maxFileSize, 10),
        limit: parseInt(opts.limit, 10),
      });
    });

  program
    .command('all')
    .description('Run all three source types')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('--batch-size <n>', 'Messages per batch', '50')
    .option('--dry-run', 'Parse and chunk without sending to Kafka', false)
    .option('--skills-dir <dir>', 'Skills directory')
    .option('--code-dir <dir>', 'Code project directory')
    .option('--sessions-dir <dir>', 'Claude directory (default: ~/.claude)')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (opts) => {
      applyLogLevel(opts);
      await runAll({
        brokers: opts.brokers,
        batchSize: parseInt(opts.batchSize, 10),
        dryRun: opts.dryRun,
        skillsDir: opts.skillsDir,
        codeDir: opts.codeDir,
        sessionsDir: opts.sessionsDir,
      });
    });

  program
    .command('consume')
    .description('Consume and process messages from Kafka topics')
    .argument('<type>', 'Source type: skills | sessions | code | all')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('--group-id <id>', 'Consumer group ID')
    .option('--batch-size <n>', 'Messages per processing batch', '50')
    .option('--from-beginning', 'Start from earliest offset', true)
    .option('--no-from-beginning', 'Start from latest offset')
    .option('--dry-run', 'Process without producing output', false)
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (type: string, opts) => {
      applyLogLevel(opts);
      await runConsume(type, {
        brokers: opts.brokers,
        groupId: opts.groupId,
        batchSize: parseInt(opts.batchSize, 10),
        fromBeginning: opts.fromBeginning,
        dryRun: opts.dryRun,
      });
    });

  program
    .command('serve')
    .description('Start HTTP ingestion server (receives chunks, produces to Kafka)')
    .option('-p, --port <port>', 'Server port', '3000')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (opts) => {
      applyLogLevel(opts);
      await startServer({
        port: parseInt(opts.port, 10),
        brokers: opts.brokers,
      });
    });

  program
    .command('setup')
    .description('Verify Kafka connectivity, provision topics, run hello-world test')
    .option('-b, --brokers <hosts>', 'Kafka broker addresses', 'localhost:9092')
    .option('-v, --verbose', 'Verbose logging', false)
    .option('-q, --quiet', 'Suppress all output except errors', false)
    .action(async (opts) => {
      applyLogLevel(opts);
      await runSetup({ brokers: opts.brokers });
    });

  return program;
}

function applyLogLevel(opts: { verbose?: boolean; quiet?: boolean }) {
  if (opts.quiet) setLogLevel('error');
  else if (opts.verbose) setLogLevel('debug');
}
