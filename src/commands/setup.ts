import { Kafka } from 'kafkajs';
import { buildKafkaConfig } from '../kafka/config';
import { provisionTopics, ALL_TOPICS } from '../kafka/admin';
import { setupPubSub } from '../pubsub/setup';
import logger from '../utils/logger';

export interface SetupCommandOptions {
  brokers: string;
  pubsub?: string;
}

export async function runSetup(options: SetupCommandOptions): Promise<void> {
  if (options.pubsub) {
    logger.info('=== claude-sink setup (Pub/Sub) ===');
    logger.info('');
    await setupPubSub(options.pubsub);
    logger.info('');
    logger.info('Produce:  claude-sink skills <dir> --pubsub ' + options.pubsub);
    logger.info('Consume:  claude-sink consume skills --pubsub ' + options.pubsub);
    return;
  }

  logger.info('=== claude-sink setup (Kafka) ===');
  logger.info('');

  logger.info('1. Testing Kafka connectivity...');
  const kafkaConfig = buildKafkaConfig(options.brokers);
  const kafka = new Kafka({
    clientId: kafkaConfig.clientId,
    brokers: kafkaConfig.brokers,
    connectionTimeout: kafkaConfig.connectionTimeout,
    retry: { initialRetryTime: 100, retries: 3 },
  });

  const admin = kafka.admin();
  try {
    await admin.connect();
  } catch (err) {
    logger.error(`   Cannot connect to Kafka at ${options.brokers}`);
    logger.error(`   ${(err as Error).message}`);
    process.exit(1);
  }
  logger.info('   Connected to Kafka');

  logger.info('');
  logger.info('2. Provisioning topics...');
  await provisionTopics(kafkaConfig);

  logger.info('');
  logger.info('3. Verifying topics...');
  const topics = await admin.listTopics();
  const expected = ALL_TOPICS.map(t => t.topic);
  const missing = expected.filter(t => !topics.includes(t));
  if (missing.length > 0) {
    logger.error(`   Missing topics: ${missing.join(', ')}`);
    process.exit(1);
  }
  logger.info(`   All ${expected.length} topics exist`);
  await admin.disconnect();

  logger.info('');
  logger.info('=== Setup complete ===');
}
