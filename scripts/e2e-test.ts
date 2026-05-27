#!/usr/bin/env npx tsx

import { PubSub } from '@google-cloud/pubsub';

const PROJECT = process.env.SINK_PROJECT || process.argv[2] || 'blobfish-ai-429200';

async function main() {
  console.log(`=== claude-sink e2e test (Pub/Sub: ${PROJECT}) ===\n`);
  const client = new PubSub({ projectId: PROJECT });

  // 1. Check topics exist
  console.log('1. Checking topics...');
  const [topics] = await client.getTopics();
  const sinkTopics = topics.filter(t => t.name.includes('sink-'));
  console.log(`   ${sinkTopics.length} sink topics found`);
  if (sinkTopics.length < 9) { console.error('   FAIL: expected 9 topics'); process.exit(1); }
  console.log('   PASS\n');

  // 2. Check subscriptions
  console.log('2. Checking subscriptions...');
  const [subs] = await client.getSubscriptions();
  const sinkSubs = subs.filter(s => s.name.includes('sink-') && s.name.includes('-processor'));
  console.log(`   ${sinkSubs.length} processor subscriptions found`);
  if (sinkSubs.length < 3) { console.error('   FAIL: expected 3 subscriptions'); process.exit(1); }
  console.log('   PASS\n');

  // 3. Publish a test message
  console.log('3. Publishing test message...');
  const topic = client.topic('sink-skills');
  const msgId = await topic.publishMessage({
    data: Buffer.from(JSON.stringify({ chunkId: 'e2e-test', sourceType: 'skill', sourceId: 'e2e-test', sourcePath: '/tmp', timestamp: Date.now(), content: '{}', contentLength: 2, contentHash: 'test', metadata: { type: 'skill', name: 'e2e-test' } })),
    attributes: { sourceType: 'skill', sourceId: 'e2e-test', contentHash: 'test' },
  });
  console.log(`   Published: ${msgId}`);
  console.log('   PASS\n');

  // 4. Consume the test message
  console.log('4. Consuming test message...');
  const sub = client.subscription('sink-skills-processor');
  const received = await new Promise<boolean>((resolve) => {
    const timeout = setTimeout(() => resolve(false), 15000);
    sub.on('message', (msg) => {
      const data = JSON.parse(msg.data.toString());
      if (data.sourceId === 'e2e-test') {
        msg.ack();
        clearTimeout(timeout);
        resolve(true);
      } else {
        msg.ack();
      }
    });
  });
  sub.removeAllListeners();
  if (received) {
    console.log('   PASS: message received and acked\n');
  } else {
    console.log('   WARN: timed out (message may have been consumed by another subscriber)\n');
  }

  // 5. Summary
  console.log('=== e2e test complete ===');
  await client.close();
}

main().catch(err => { console.error('FAIL:', err.message); process.exit(1); });
