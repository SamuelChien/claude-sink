#!/usr/bin/env npx tsx

import { buildKafkaConfig } from '../src/kafka/config';
import { provisionTopics, ALL_TOPICS } from '../src/kafka/admin';

async function main() {
  const brokers = process.env.KAFKA_BROKERS || 'localhost:9092';
  console.log(`Provisioning ${ALL_TOPICS.length} topics on ${brokers}...\n`);

  const kafkaConfig = buildKafkaConfig(brokers);
  await provisionTopics(kafkaConfig);

  console.log('\nDone.');
}

main().catch(err => {
  console.error('Failed:', err.message);
  process.exit(1);
});
