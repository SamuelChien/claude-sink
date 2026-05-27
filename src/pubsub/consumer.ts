import { PubSub, Subscription, Message } from '@google-cloud/pubsub';
import logger from '../utils/logger';
import type { ConsumptionChunk } from '../types/chunk';

export interface PubSubConsumerConfig {
  projectId: string;
  inputTopic: string;
  outputTopic: string;
  subscriptionName: string;
  dryRun: boolean;
}

export interface ProcessingResult<T> {
  key: string;
  value: T;
}

export abstract class PubSubConsumer<TOutput> {
  private client: PubSub;
  private subscription: Subscription;
  private outputTopic: ReturnType<PubSub['topic']>;
  private config: PubSubConsumerConfig;
  private stats = { consumed: 0, processed: 0, failed: 0 };

  constructor(config: PubSubConsumerConfig) {
    this.config = config;
    this.client = new PubSub({ projectId: config.projectId });
    this.subscription = this.client.subscription(config.subscriptionName);
    this.outputTopic = this.client.topic(config.outputTopic);
  }

  abstract process(batch: ConsumptionChunk[]): ProcessingResult<TOutput>[];
  abstract get name(): string;

  async start(): Promise<void> {
    await this.ensureSubscription();
    await this.ensureOutputTopic();

    logger.info(`[${this.name}] consuming ${this.config.inputTopic} → ${this.config.outputTopic}`);

    this.subscription.on('message', async (message: Message) => {
      try {
        const chunk: ConsumptionChunk = JSON.parse(message.data.toString());
        this.stats.consumed++;

        const results = this.process([chunk]);

        if (!this.config.dryRun && results.length > 0) {
          const publishPromises = results.map(r =>
            this.outputTopic.publishMessage({
              data: Buffer.from(JSON.stringify(r.value)),
              attributes: { sourceId: r.key },
            })
          );
          await Promise.all(publishPromises);
        }

        this.stats.processed++;
        message.ack();

        if (this.stats.processed % 100 === 0) {
          logger.info(`[${this.name}] processed: ${this.stats.processed}, failed: ${this.stats.failed}`);
        }
      } catch (err) {
        this.stats.failed++;
        logger.warn(`[${this.name}] failed: ${(err as Error).message}`);
        message.nack();
      }
    });

    this.subscription.on('error', (err: Error) => {
      logger.error(`[${this.name}] subscription error: ${err.message}`);
    });

    const shutdown = async (signal: string) => {
      logger.info(`[${this.name}] ${signal} received, shutting down...`);
      this.subscription.removeAllListeners();
      await this.client.close();
      logger.info(`[${this.name}] shutdown complete. stats: ${JSON.stringify(this.stats)}`);
      process.exit(0);
    };
    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  private async ensureSubscription(): Promise<void> {
    const [exists] = await this.subscription.exists();
    if (!exists) {
      const topic = this.client.topic(this.config.inputTopic);
      const [topicExists] = await topic.exists();
      if (!topicExists) await this.client.createTopic(this.config.inputTopic);
      await topic.createSubscription(this.config.subscriptionName);
      logger.info(`Created subscription: ${this.config.subscriptionName}`);
    }
  }

  private async ensureOutputTopic(): Promise<void> {
    const [exists] = await this.outputTopic.exists();
    if (!exists) {
      await this.client.createTopic(this.config.outputTopic);
      logger.info(`Created output topic: ${this.config.outputTopic}`);
    }
  }
}
