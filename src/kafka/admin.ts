import { Kafka } from 'kafkajs';
import logger from '../utils/logger';
import type { KafkaConfig } from './config';

export interface TopicSpec {
  topic: string;
  numPartitions: number;
  replicationFactor: number;
}

export const ALL_TOPICS: TopicSpec[] = [
  { topic: 'sink.skills', numPartitions: 6, replicationFactor: 1 },
  { topic: 'sink.sessions', numPartitions: 3, replicationFactor: 1 },
  { topic: 'sink.code', numPartitions: 6, replicationFactor: 1 },
  { topic: 'sink.skills.analyzed', numPartitions: 6, replicationFactor: 1 },
  { topic: 'sink.sessions.analyzed', numPartitions: 3, replicationFactor: 1 },
  { topic: 'sink.code.analyzed', numPartitions: 6, replicationFactor: 1 },
  { topic: 'sink.skills.dlq', numPartitions: 1, replicationFactor: 1 },
  { topic: 'sink.sessions.dlq', numPartitions: 1, replicationFactor: 1 },
  { topic: 'sink.code.dlq', numPartitions: 1, replicationFactor: 1 },
];

export async function provisionTopics(kafkaConfig: KafkaConfig, topics: TopicSpec[] = ALL_TOPICS): Promise<void> {
  const kafka = new Kafka({
    clientId: kafkaConfig.clientId,
    brokers: kafkaConfig.brokers,
    connectionTimeout: kafkaConfig.connectionTimeout,
    retry: kafkaConfig.retry,
  });

  const admin = kafka.admin();
  await admin.connect();

  try {
    const existing = await admin.listTopics();
    const toCreate = topics.filter(t => !existing.includes(t.topic));
    const toResize = topics.filter(t => existing.includes(t.topic));

    if (toCreate.length > 0) {
      await admin.createTopics({ topics: toCreate });
      for (const t of toCreate) {
        logger.info(`Created topic: ${t.topic} (${t.numPartitions} partitions)`);
      }
    }

    for (const t of toResize) {
      try {
        const metadata = await admin.fetchTopicMetadata({ topics: [t.topic] });
        const currentPartitions = metadata.topics[0]?.partitions.length || 0;
        if (currentPartitions < t.numPartitions) {
          await admin.createPartitions({ topicPartitions: [{ topic: t.topic, count: t.numPartitions }] });
          logger.info(`Resized topic: ${t.topic} (${currentPartitions} → ${t.numPartitions} partitions)`);
        } else {
          logger.info(`Topic exists: ${t.topic} (${currentPartitions} partitions)`);
        }
      } catch (err) {
        logger.warn(`Could not resize ${t.topic}: ${(err as Error).message}`);
      }
    }
  } finally {
    await admin.disconnect();
  }
}
