import { PubSub, Topic } from '@google-cloud/pubsub';
import logger from '../utils/logger';
import type { ConsumptionChunk } from '../types/chunk';

export interface PubSubProducerConfig {
  projectId: string;
  batchSize: number;
  topic: string;
  dryRun: boolean;
}

export class PubSubProducer {
  private client: PubSub;
  private topic: Topic;
  private config: PubSubProducerConfig;

  constructor(config: PubSubProducerConfig) {
    this.config = config;
    this.client = new PubSub({ projectId: config.projectId });
    this.topic = this.client.topic(config.topic, {
      batching: { maxMessages: config.batchSize, maxMilliseconds: 100 },
      gaxOpts: { timeout: 30000 },
    });
  }

  async connect(): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[dry-run] Skipping Pub/Sub connection');
      return;
    }
    const [exists] = await this.topic.exists();
    if (!exists) {
      await this.client.createTopic(this.config.topic);
      logger.info(`Created topic: ${this.config.topic}`);
    }
    logger.info(`Connected to Pub/Sub topic: ${this.config.topic}`);
  }

  async disconnect(): Promise<void> {
    await this.client.close();
  }

  async sendChunks(chunks: ConsumptionChunk[]): Promise<number> {
    let sent = 0;

    for (let i = 0; i < chunks.length; i += this.config.batchSize) {
      const batch = chunks.slice(i, i + this.config.batchSize);

      if (this.config.dryRun) {
        for (const chunk of batch) {
          logger.info(`[dry-run] ${chunk.sourceType} | ${chunk.sourceId} | ${chunk.contentLength} bytes`);
        }
        sent += batch.length;
        continue;
      }

      const publishPromises = batch.map(chunk =>
        this.topic.publishMessage({
          data: Buffer.from(JSON.stringify(chunk)),
          attributes: {
            sourceType: chunk.sourceType,
            sourceId: chunk.sourceId,
            contentHash: chunk.contentHash,
          },
        })
      );

      await Promise.all(publishPromises);
      sent += batch.length;
      const progress = Math.min(i + this.config.batchSize, chunks.length);
      logger.info(`Sent batch: ${batch.length} chunks (${progress}/${chunks.length})`);
    }

    return sent;
  }
}
