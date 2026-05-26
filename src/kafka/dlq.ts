import { Kafka, Producer } from 'kafkajs';
import logger from '../utils/logger';
import type { KafkaConfig } from './config';
import type { ConsumptionChunk } from '../types/chunk';

export class DlqProducer {
  private producer: Producer;
  private connected = false;
  private topic: string;

  constructor(kafka: Kafka, topic: string) {
    this.topic = topic;
    this.producer = kafka.producer();
  }

  async connect(): Promise<void> {
    await this.producer.connect();
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    if (this.connected) {
      await this.producer.disconnect();
      this.connected = false;
    }
  }

  async send(chunk: ConsumptionChunk, error: Error, originalTopic: string, partition: number, offset: string): Promise<void> {
    if (!this.connected) return;

    try {
      await this.producer.send({
        topic: this.topic,
        messages: [{
          key: chunk.sourceId,
          value: JSON.stringify(chunk),
          headers: {
            'error-message': error.message,
            'original-topic': originalTopic,
            'original-partition': partition.toString(),
            'original-offset': offset,
            'failed-at': new Date().toISOString(),
          },
        }],
      });
      logger.warn(`DLQ: sent ${chunk.sourceId} to ${this.topic}: ${error.message}`);
    } catch (dlqErr) {
      logger.error(`DLQ send failed for ${chunk.sourceId}: ${(dlqErr as Error).message}`);
    }
  }
}
