#!/usr/bin/env npx tsx

import { Kafka } from 'kafkajs';

const TEST_TOPIC = 'sink.test';
const BROKER = process.env.KAFKA_BROKERS || 'localhost:9092';

async function main() {
  console.log('=== claude-sink hello world e2e test ===\n');

  const kafka = new Kafka({
    clientId: 'claude-sink-test',
    brokers: [BROKER],
    retry: { initialRetryTime: 100, retries: 3 },
  });

  const admin = kafka.admin();
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: 'test-group-' + Date.now() });

  try {
    // 1. Connect
    console.log('1. Connecting to Kafka at', BROKER);
    await admin.connect();
    await producer.connect();
    await consumer.connect();
    console.log('   CONNECTED\n');

    // 2. List existing topics
    const topics = await admin.listTopics();
    console.log('2. Existing topics:', topics.join(', '));
    for (const t of ['sink.skills', 'sink.sessions', 'sink.code']) {
      if (topics.includes(t)) {
        const offsets = await admin.fetchTopicOffsets(t);
        const total = offsets.reduce((sum, p) => sum + parseInt(p.high), 0);
        console.log(`   ${t}: ${total} messages`);
      }
    }
    console.log();

    // 3. Produce a test message
    const testMessage = {
      chunkId: 'test-hello-world',
      sourceType: 'skill' as const,
      sourceId: 'hello-world-test',
      sourcePath: '/tmp/test',
      timestamp: Date.now(),
      content: JSON.stringify({ greeting: 'hello from claude-sink!', time: new Date().toISOString() }),
      contentLength: 64,
      contentHash: 'test-hash',
      metadata: { type: 'skill', name: 'hello-world-test', description: 'e2e test', tags: [], category: 'test', author: 'claude-sink', platforms: [], allowedTools: [], tier: 'test', version: '1.0.0', headingCount: 0, codeBlockCount: 0, linkCount: 0, sourceCollection: 'test' },
    };

    console.log('3. Producing test message to', TEST_TOPIC);
    await producer.send({
      topic: TEST_TOPIC,
      messages: [{ key: 'hello-world', value: JSON.stringify(testMessage) }],
    });
    console.log('   PRODUCED\n');

    // 4. Consume it back
    console.log('4. Consuming from', TEST_TOPIC);
    await consumer.subscribe({ topic: TEST_TOPIC, fromBeginning: true });

    const received = await new Promise<string>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error('Consume timeout')), 10000);
      consumer.run({
        eachMessage: async ({ message }) => {
          clearTimeout(timeout);
          resolve(message.value!.toString());
        },
      });
    });

    const parsed = JSON.parse(received);
    console.log('   CONSUMED');
    console.log(`   sourceType: ${parsed.sourceType}`);
    console.log(`   sourceId:   ${parsed.sourceId}`);
    console.log(`   content:    ${parsed.content}`);
    console.log();

    // 5. Verify round-trip
    const match = parsed.chunkId === testMessage.chunkId;
    console.log(`5. Round-trip verification: ${match ? 'PASS' : 'FAIL'}`);
    console.log();
    console.log('=== e2e test complete ===');

    await admin.deleteTopics({ topics: [TEST_TOPIC] }).catch(() => {});
  } finally {
    await consumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
  }
}

main().catch(err => {
  console.error('TEST FAILED:', err.message);
  process.exit(1);
});
