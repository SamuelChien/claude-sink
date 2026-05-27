import fs from 'fs';
import path from 'path';
import { Kafka, Producer } from 'kafkajs';
import logger from '../utils/logger';
import type { KafkaConfig } from './config';
import type { ConsumptionChunk } from '../types/chunk';

const FALLBACK_FILE = path.join(process.env.HOME || '/tmp', '.claude-sink', 'dlq-fallback.jsonl');

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
      this.writeFallback(chunk, error, originalTopic, partition, offset);
    }
  }

  private writeFallback(chunk: ConsumptionChunk, error: Error, originalTopic: string, partition: number, offset: string): void {
    try {
      const dir = path.dirname(FALLBACK_FILE);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      const entry = JSON.stringify({
        sourceId: chunk.sourceId,
        sourceType: chunk.sourceType,
        error: error.message,
        originalTopic,
        partition,
        offset,
        failedAt: new Date().toISOString(),
      });
      fs.appendFileSync(FALLBACK_FILE, entry + '\n');
      logger.warn(`DLQ fallback: wrote ${chunk.sourceId} to ${FALLBACK_FILE}`);
    } catch (fileErr) {
      logger.error(`DLQ fallback file write failed: ${(fileErr as Error).message}`);
    }
  }
}
