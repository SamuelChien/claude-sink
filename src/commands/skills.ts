import { SkillsReader } from '../readers/skills-reader';
import { chunkSkill } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { SinkHttpClient } from '../server/client';
import { buildKafkaConfig } from '../kafka/config';
import { filterNew, markProduced, commitState } from '../utils/state';
import logger from '../utils/logger';

export interface SkillsCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  maxDepth: number;
  limit: number;
  server?: string;
  force?: boolean;
}

export async function runSkills(dir: string, options: SkillsCommandOptions): Promise<void> {
  const reader = new SkillsReader(dir, {
    maxDepth: options.maxDepth,
    limit: options.limit,
  });

  const skills = await reader.readAll();
  const allChunks = skills.map(chunkSkill);

  const chunks = options.force ? allChunks : dedup(options.topic, allChunks);
  logger.info(`Chunked ${allChunks.length} skills, ${allChunks.length - chunks.length} unchanged, ${chunks.length} new → topic "${options.topic}"`);

  if (chunks.length === 0) { logger.info('Nothing new to produce.'); return; }

  const sent = await send(chunks, options);
  for (const c of chunks) markProduced(options.topic, c.sourceId, c.contentHash);
  commitState();
  logger.info(`Done: ${sent} skill chunks sent`);
}

function dedup(topic: string, chunks: { sourceId: string; contentHash: string }[]) {
  const newIdx = filterNew(topic, chunks);
  return newIdx.map(i => chunks[i]);
}

async function send(chunks: any[], options: SkillsCommandOptions): Promise<number> {
  if (options.server) {
    const client = new SinkHttpClient(options.server, options.batchSize);
    return client.sendChunks(options.topic, chunks);
  }
  const kafkaConfig = buildKafkaConfig(options.brokers);
  const producer = new SinkProducer(kafkaConfig, { batchSize: options.batchSize, topic: options.topic, dryRun: options.dryRun });
  try {
    await producer.connect();
    return await producer.sendChunks(chunks);
  } finally {
    await producer.disconnect();
  }
}
