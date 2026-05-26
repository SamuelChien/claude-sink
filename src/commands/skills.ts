import { SkillsReader } from '../readers/skills-reader';
import { chunkSkill } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { buildKafkaConfig } from '../kafka/config';
import logger from '../utils/logger';

export interface SkillsCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  maxDepth: number;
  limit: number;
}

export async function runSkills(dir: string, options: SkillsCommandOptions): Promise<void> {
  const reader = new SkillsReader(dir, {
    maxDepth: options.maxDepth,
    limit: options.limit,
  });

  const kafkaConfig = buildKafkaConfig(options.brokers);
  const producer = new SinkProducer(kafkaConfig, {
    batchSize: options.batchSize,
    topic: options.topic,
    dryRun: options.dryRun,
  });

  try {
    await producer.connect();
    const skills = await reader.readAll();
    const chunks = skills.map(chunkSkill);

    logger.info(`Chunked ${chunks.length} skills → topic "${options.topic}"`);
    const sent = await producer.sendChunks(chunks);
    logger.info(`Done: ${sent} skill chunks sent`);
  } finally {
    await producer.disconnect();
  }
}
