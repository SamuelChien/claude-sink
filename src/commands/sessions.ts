import { SessionsReader } from '../readers/sessions-reader';
import { chunkSession } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { buildKafkaConfig } from '../kafka/config';
import logger from '../utils/logger';

export interface SessionsCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  limit: number;
  since?: string;
  project?: string;
}

export async function runSessions(dir: string | undefined, options: SessionsCommandOptions): Promise<void> {
  const reader = new SessionsReader(dir, {
    limit: options.limit,
    since: options.since,
    project: options.project,
  });

  const kafkaConfig = buildKafkaConfig(options.brokers);
  const producer = new SinkProducer(kafkaConfig, {
    batchSize: options.batchSize,
    topic: options.topic,
    dryRun: options.dryRun,
  });

  try {
    await producer.connect();
    const sessions = await reader.readAll();
    const chunks = sessions.flatMap(chunkSession);

    logger.info(`Chunked ${sessions.length} sessions → ${chunks.length} chunks → topic "${options.topic}"`);
    const sent = await producer.sendChunks(chunks);
    logger.info(`Done: ${sent} session chunks sent`);
  } finally {
    await producer.disconnect();
  }
}
