import { SkillsReader } from '../readers/skills-reader';
import { chunkSkill } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { SinkHttpClient } from '../server/client';
import { buildKafkaConfig } from '../kafka/config';
import logger from '../utils/logger';

export interface SkillsCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  maxDepth: number;
  limit: number;
  server?: string;
}

export async function runSkills(dir: string, options: SkillsCommandOptions): Promise<void> {
  const reader = new SkillsReader(dir, {
    maxDepth: options.maxDepth,
    limit: options.limit,
  });

  const skills = await reader.readAll();
  const chunks = skills.map(chunkSkill);
  logger.info(`Chunked ${chunks.length} skills → topic "${options.topic}"`);

  if (options.server) {
    const client = new SinkHttpClient(options.server, options.batchSize);
    const sent = await client.sendChunks(options.topic, chunks);
    logger.info(`Done: ${sent} skill chunks sent via ${options.server}`);
    return;
  }

  const kafkaConfig = buildKafkaConfig(options.brokers);
  const producer = new SinkProducer(kafkaConfig, {
    batchSize: options.batchSize,
    topic: options.topic,
    dryRun: options.dryRun,
  });

  try {
    await producer.connect();
    const sent = await producer.sendChunks(chunks);
    logger.info(`Done: ${sent} skill chunks sent`);
  } finally {
    await producer.disconnect();
  }
}
