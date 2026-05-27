import { PubSub } from '@google-cloud/pubsub';
import logger from '../utils/logger';

const TOPICS = [
  'sink-skills',
  'sink-sessions',
  'sink-code',
  'sink-skills-analyzed',
  'sink-sessions-analyzed',
  'sink-code-analyzed',
  'sink-skills-dlq',
  'sink-sessions-dlq',
  'sink-code-dlq',
];

const SUBSCRIPTIONS: Record<string, string> = {
  'sink-skills': 'sink-skills-processor',
  'sink-sessions': 'sink-sessions-processor',
  'sink-code': 'sink-code-processor',
};

export async function setupPubSub(projectId: string): Promise<void> {
  const client = new PubSub({ projectId });

  logger.info(`Setting up Pub/Sub in project: ${projectId}`);

  const [existingTopics] = await client.getTopics();
  const existingNames = new Set(existingTopics.map(t => t.name.split('/').pop()));

  for (const topic of TOPICS) {
    if (existingNames.has(topic)) {
      logger.info(`Topic exists: ${topic}`);
    } else {
      await client.createTopic(topic);
      logger.info(`Created topic: ${topic}`);
    }
  }

  const [existingSubs] = await client.getSubscriptions();
  const existingSubNames = new Set(existingSubs.map(s => s.name.split('/').pop()));

  for (const [topic, sub] of Object.entries(SUBSCRIPTIONS)) {
    if (existingSubNames.has(sub)) {
      logger.info(`Subscription exists: ${sub}`);
    } else {
      await client.topic(topic).createSubscription(sub, {
        ackDeadlineSeconds: 60,
        messageRetentionDuration: { seconds: 604800 },
      });
      logger.info(`Created subscription: ${sub} → ${topic}`);
    }
  }

  await client.close();
  logger.info('Setup complete.');
}
