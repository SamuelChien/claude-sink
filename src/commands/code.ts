import { CodeReader } from '../readers/code-reader';
import { chunkCodeFile } from '../chunking/chunker';
import { SinkProducer } from '../kafka/producer';
import { buildKafkaConfig } from '../kafka/config';
import logger from '../utils/logger';

export interface CodeCommandOptions {
  brokers: string;
  topic: string;
  batchSize: number;
  dryRun: boolean;
  exclude?: string[];
  maxFileSize: number;
  limit: number;
}

export async function runCode(dir: string, options: CodeCommandOptions): Promise<void> {
  const reader = new CodeReader(dir, {
    exclude: options.exclude,
    maxFileSize: options.maxFileSize,
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
    const files = await reader.readAll();
    const chunks = files.map(chunkCodeFile);

    logger.info(`Chunked ${chunks.length} code files → topic "${options.topic}"`);
    const sent = await producer.sendChunks(chunks);
    logger.info(`Done: ${sent} code chunks sent`);
  } finally {
    await producer.disconnect();
  }
}
