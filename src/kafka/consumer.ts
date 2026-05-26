import { Kafka, Consumer, Producer, EachBatchPayload } from 'kafkajs';
import logger from '../utils/logger';
import { DlqProducer } from './dlq';
import type { KafkaConfig, ConsumerConfig } from './config';
import type { ConsumptionChunk } from '../types/chunk';

export interface ProcessingResult<T> {
  key: string;
  value: T;
}

export abstract class SinkConsumer<TOutput> {
  private kafka: Kafka;
  private consumer: Consumer;
  private producer: Producer;
  private dlq: DlqProducer;
  private config: ConsumerConfig;
  private shuttingDown = false;

  private stats = { consumed: 0, processed: 0, failed: 0, batches: 0 };

  constructor(kafkaConfig: KafkaConfig, consumerConfig: ConsumerConfig) {
    this.config = consumerConfig;
    this.kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers,
      connectionTimeout: kafkaConfig.connectionTimeout,
      retry: kafkaConfig.retry,
    });
    this.consumer = this.kafka.consumer({
      groupId: consumerConfig.groupId,
      sessionTimeout: 30_000,
      heartbeatInterval: 3_000,
    });
    this.producer = this.kafka.producer({ maxInFlightRequests: 5, idempotent: true });
    this.dlq = new DlqProducer(this.kafka, consumerConfig.dlqTopic);
  }

  abstract process(batch: ConsumptionChunk[]): ProcessingResult<TOutput>[];
  abstract get name(): string;

  async start(): Promise<void> {
    this.registerShutdownHandlers();

    if (!this.config.dryRun) {
      await this.producer.connect();
      await this.dlq.connect();
    }

    await this.consumer.connect();
    await this.consumer.subscribe({
      topic: this.config.inputTopic,
      fromBeginning: this.config.fromBeginning,
    });

    logger.info(`[${this.name}] consuming ${this.config.inputTopic} → ${this.config.outputTopic} (group: ${this.config.groupId})`);

    await this.consumer.run({
      eachBatchAutoResolve: false,
      eachBatch: async (payload) => this.handleBatch(payload),
    });
  }

  private async handleBatch({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, isRunning, isStale }: EachBatchPayload): Promise<void> {
    const chunks: ConsumptionChunk[] = [];
    const offsets: { partition: number; offset: string }[] = [];

    for (const message of batch.messages) {
      if (!isRunning() || isStale() || this.shuttingDown) break;

      try {
        const chunk: ConsumptionChunk = JSON.parse(message.value!.toString());
        chunks.push(chunk);
        offsets.push({ partition: batch.partition, offset: message.offset });
      } catch (err) {
        logger.warn(`[${this.name}] failed to parse message at offset ${message.offset}: ${(err as Error).message}`);
        resolveOffset(message.offset);
      }

      if (chunks.length >= this.config.batchSize) {
        await this.processBatch(chunks, offsets, batch.topic, batch.partition, resolveOffset, heartbeat);
        chunks.length = 0;
        offsets.length = 0;
        await heartbeat();
      }
    }

    if (chunks.length > 0) {
      await this.processBatch(chunks, offsets, batch.topic, batch.partition, resolveOffset, heartbeat);
    }

    await commitOffsetsIfNecessary();
    this.stats.batches++;
  }

  private async processBatch(
    chunks: ConsumptionChunk[],
    offsets: { partition: number; offset: string }[],
    topic: string,
    partition: number,
    resolveOffset: (offset: string) => void,
    heartbeat: () => Promise<void>,
  ): Promise<void> {
    this.stats.consumed += chunks.length;

    const results: ProcessingResult<TOutput>[] = [];
    for (let i = 0; i < chunks.length; i++) {
      try {
        const batch = this.process([chunks[i]]);
        results.push(...batch);
        this.stats.processed++;
      } catch (err) {
        this.stats.failed++;
        if (!this.config.dryRun) {
          await this.dlq.send(chunks[i], err as Error, topic, partition, offsets[i].offset);
        } else {
          logger.warn(`[${this.name}] [dry-run] DLQ: ${chunks[i].sourceId} — ${(err as Error).message}`);
        }
      }
      resolveOffset(offsets[i].offset);
    }

    if (results.length === 0) return;

    if (this.config.dryRun) {
      for (const r of results) {
        logger.info(`[${this.name}] [dry-run] ${r.key} → ${this.config.outputTopic}`);
      }
      return;
    }

    const messages = results.map(r => ({
      key: r.key,
      value: JSON.stringify(r.value),
      headers: {
        'content-type': 'application/json',
        'source': `claude-sink-${this.name}`,
        'version': '1.0',
      },
    }));

    await this.producer.send({ topic: this.config.outputTopic, messages });
    await heartbeat();

    logger.info(`[${this.name}] batch: ${results.length} → ${this.config.outputTopic} (total: ${this.stats.processed}, failed: ${this.stats.failed})`);
  }

  private registerShutdownHandlers(): void {
    const shutdown = async (signal: string) => {
      if (this.shuttingDown) return;
      this.shuttingDown = true;
      logger.info(`[${this.name}] ${signal} received, shutting down gracefully...`);

      try {
        await this.consumer.disconnect();
        if (!this.config.dryRun) {
          await this.producer.disconnect();
          await this.dlq.disconnect();
        }
      } catch (err) {
        logger.error(`[${this.name}] shutdown error: ${(err as Error).message}`);
      }

      logger.info(`[${this.name}] shutdown complete. stats: ${JSON.stringify(this.stats)}`);
      process.exit(0);
    };

    process.on('SIGINT', () => shutdown('SIGINT'));
    process.on('SIGTERM', () => shutdown('SIGTERM'));
  }

  getStats() { return { ...this.stats }; }
}
