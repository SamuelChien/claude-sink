import { Kafka, Producer, CompressionTypes } from 'kafkajs';
import logger from '../utils/logger';
import type { KafkaConfig, ProducerConfig } from './config';
import type { ConsumptionChunk } from '../types/chunk';

export class SinkProducer {
  private kafka: Kafka;
  private producer: Producer;
  private connected = false;
  private config: ProducerConfig;

  constructor(kafkaConfig: KafkaConfig, producerConfig: ProducerConfig) {
    this.config = producerConfig;
    this.kafka = new Kafka({
      clientId: kafkaConfig.clientId,
      brokers: kafkaConfig.brokers,
      connectionTimeout: kafkaConfig.connectionTimeout,
      retry: kafkaConfig.retry,
    });
    this.producer = this.kafka.producer({
      maxInFlightRequests: 5,
      idempotent: true,
    });
  }

  async connect(): Promise<void> {
    if (this.config.dryRun) {
      logger.info('[dry-run] Skipping Kafka connection');
      return;
    }
    await this.producer.connect();
    this.connected = true;
    logger.info(`Connected to Kafka, topic: ${this.config.topic}`);
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
    }
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

      if (!this.connected) throw new Error('Producer not connected');

      const messages = batch.map(chunk => ({
        key: chunk.sourceId,
        value: JSON.stringify(chunk),
        timestamp: chunk.timestamp.toString(),
        headers: {
          'content-type': 'application/json',
          'source': 'claude-sink',
          'source-type': chunk.sourceType,
          'version': '1.0',
        },
      }));

      await this.producer.send({
        topic: this.config.topic,
        messages,
      });

      sent += batch.length;
      const progress = Math.min(i + this.config.batchSize, chunks.length);
      logger.info(`Sent batch: ${batch.length} chunks (${progress}/${chunks.length})`);
    }

    return sent;
  }
}
