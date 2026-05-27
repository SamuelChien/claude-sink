import { SessionsReader } from '../readers/sessions-reader';
import { chunkSession } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { PubSubProducer } from '../pubsub/producer';
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
  pubsub?: string;
}

export async function runSessions(dir: string | undefined, options: SessionsCommandOptions): Promise<void> {
  const reader = new SessionsReader(dir, { limit: options.limit, since: options.since, project: options.project });
  const sessions = await reader.readAll();
  const topic = options.pubsub ? options.topic.replace(/\./g, '-') : options.topic;
  const allChunks = sessions.flatMap(chunkSession);
  const chunks = options.force ? allChunks : dedup(topic, allChunks);
  logger.info(`Chunked ${sessions.length} sessions → ${allChunks.length} chunks, ${allChunks.length - chunks.length} unchanged, ${chunks.length} new`);
  if (chunks.length === 0) { logger.info('Nothing new to produce.'); return; }

  let sent: number;
  if (options.pubsub) {
    const producer = new PubSubProducer({ projectId: options.pubsub, batchSize: options.batchSize, topic, dryRun: options.dryRun });
    await producer.connect();
    sent = await producer.sendChunks(chunks);
    await producer.disconnect();
  } else if (options.server) {
    sent = await new SinkHttpClient(options.server, options.batchSize).sendChunks(topic, chunks);
  } else {
    const producer = new SinkProducer(buildKafkaConfig(options.brokers), { batchSize: options.batchSize, topic, dryRun: options.dryRun });
    try { await producer.connect(); sent = await producer.sendChunks(chunks); } finally { await producer.disconnect(); }
  }

  for (const c of chunks) markProduced(topic, c.sourceId, c.contentHash);
  commitState();
  logger.info(`Done: ${sent!} session chunks sent`);
}

function dedup<T extends { sourceId: string; contentHash: string }>(topic: string, chunks: T[]): T[] {
  return filterNew(topic, chunks).map(i => chunks[i]);
}
