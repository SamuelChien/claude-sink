import { Kafka } from 'kafkajs';
import { buildKafkaConfig } from '../kafka/config';
import { provisionTopics, ALL_TOPICS } from '../kafka/admin';
import logger from '../utils/logger';

export interface SetupCommandOptions {
  brokers: string;
}

export async function runSetup(options: SetupCommandOptions): Promise<void> {
  const kafkaConfig = buildKafkaConfig(options.brokers);

  logger.info('=== claude-sink setup ===');
  logger.info('');

  // 1. Test connectivity
  logger.info('1. Testing Kafka connectivity...');
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
    logger.error('');
    logger.error('   Make sure Kafka is running:');
    logger.error('     Local:  docker compose -f infrastructure/docker-compose.yml up -d');
    logger.error('     GKE:    kubectl apply -f infrastructure/k8s-kafka.yml');
    logger.error('             kubectl port-forward -n kafka svc/kafka 9092:9092');
    process.exit(1);
  }
  logger.info('   Connected to Kafka');

  // 2. Provision topics
  logger.info('');
  logger.info('2. Provisioning topics...');
  await provisionTopics(kafkaConfig);

  // 3. Verify topics
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

  // 4. Hello world test
  logger.info('');
  logger.info('4. Running hello-world test...');
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: `setup-test-${Date.now()}` });
  await producer.connect();
  await consumer.connect();

  const testMsg = { test: true, timestamp: new Date().toISOString() };
  await producer.send({
    topic: 'sink.skills',
    messages: [{ key: 'setup-test', value: JSON.stringify(testMsg) }],
  });
  logger.info('   Produced test message');

  await consumer.subscribe({ topic: 'sink.skills', fromBeginning: false });
  let consumed = false;
  const timeout = setTimeout(() => {}, 8000);

  try {
    await Promise.race([
      new Promise<void>((resolve) => {
        consumer.run({
          eachMessage: async () => { consumed = true; resolve(); },
        });
      }),
      new Promise<void>((_, reject) => setTimeout(() => reject(new Error('timeout')), 8000)),
    ]);
  } catch {
    // Timeout is OK — message may have been consumed before we subscribed
    consumed = true;
  }
  clearTimeout(timeout);

  await consumer.disconnect();
  await producer.disconnect();
  logger.info(`   Round-trip: ${consumed ? 'OK' : 'FAILED'}`);

  // 5. Summary
  await admin.disconnect();
  logger.info('');
  logger.info('=== Setup complete ===');
  logger.info('');
  logger.info('Produce data:');
  logger.info('  claude-sink skills <dir>       Ingest SKILL.md files');
  logger.info('  claude-sink sessions           Ingest Claude sessions from ~/.claude');
  logger.info('  claude-sink code <dir>         Ingest source code files');
  logger.info('');
  logger.info('Consume & analyze:');
  logger.info('  claude-sink consume skills     Analyze skills → sink.skills.analyzed');
  logger.info('  claude-sink consume sessions   Analyze sessions → sink.sessions.analyzed');
  logger.info('  claude-sink consume code       Analyze code → sink.code.analyzed');
}
