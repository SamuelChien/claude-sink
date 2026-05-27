import { SessionsReader } from '../readers/sessions-reader';
import { chunkSession } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { SinkHttpClient } from '../server/client';
import { buildKafkaConfig } from '../kafka/config';
import { filterNew, markProduced, commitState } from '../utils/state';
import logger from '../utils/logger';

export interface SessionsCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  limit: number;
  since?: string;
  project?: string;
  server?: string;
  force?: boolean;
}

export async function runSessions(dir: string | undefined, options: SessionsCommandOptions): Promise<void> {
  const reader = new SessionsReader(dir, {
    limit: options.limit,
    since: options.since,
    project: options.project,
  });

  const sessions = await reader.readAll();
  const allChunks = sessions.flatMap(chunkSession);

  const chunks = options.force ? allChunks : dedup(options.topic, allChunks);
  logger.info(`Chunked ${sessions.length} sessions → ${allChunks.length} chunks, ${allChunks.length - chunks.length} unchanged, ${chunks.length} new → topic "${options.topic}"`);

  if (chunks.length === 0) { logger.info('Nothing new to produce.'); return; }

  const sent = await send(chunks, options);
  for (const c of chunks) markProduced(options.topic, c.sourceId, c.contentHash);
  commitState();
  logger.info(`Done: ${sent} session chunks sent`);
}

function dedup(topic: string, chunks: { sourceId: string; contentHash: string }[]) {
  const newIdx = filterNew(topic, chunks);
  return newIdx.map(i => chunks[i]);
}

async function send(chunks: any[], options: SessionsCommandOptions): Promise<number> {
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
