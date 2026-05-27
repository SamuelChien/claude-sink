import { SkillsConsumer } from '../consumers/skills-consumer';
import { SessionsConsumer } from '../consumers/sessions-consumer';
import { CodeConsumer } from '../consumers/code-consumer';
import { PubSubSkillsConsumer, PubSubSessionsConsumer, PubSubCodeConsumer } from '../pubsub/consumers';
import { buildKafkaConfig } from '../kafka/config';
import type { ConsumerConfig } from '../kafka/config';
import type { PubSubConsumerConfig } from '../pubsub/consumer';
import logger from '../utils/logger';

export interface ConsumeCommandOptions {
  brokers: string;
  groupId?: string;
  batchSize: number;
  fromBeginning: boolean;
  dryRun: boolean;
  pubsub?: string;
}

type ConsumerType = 'skills' | 'sessions' | 'code';

const SPEC = {
  skills: { input: 'sink-skills', output: 'sink-skills-analyzed', dlq: 'sink-skills-dlq', sub: 'sink-skills-processor', kafkaInput: 'sink.skills', kafkaOutput: 'sink.skills.analyzed', kafkaDlq: 'sink.skills.dlq', defaultGroup: 'sink-skills-processor' },
  sessions: { input: 'sink-sessions', output: 'sink-sessions-analyzed', dlq: 'sink-sessions-dlq', sub: 'sink-sessions-processor', kafkaInput: 'sink.sessions', kafkaOutput: 'sink.sessions.analyzed', kafkaDlq: 'sink.sessions.dlq', defaultGroup: 'sink-sessions-processor' },
  code: { input: 'sink-code', output: 'sink-code-analyzed', dlq: 'sink-code-dlq', sub: 'sink-code-processor', kafkaInput: 'sink.code', kafkaOutput: 'sink.code.analyzed', kafkaDlq: 'sink.code.dlq', defaultGroup: 'sink-code-processor' },
} as const;

export async function runConsume(type: string, options: ConsumeCommandOptions): Promise<void> {
  if (type === 'all') {
    const types: ConsumerType[] = ['skills', 'sessions', 'code'];
    await Promise.all(types.map(t => startConsumer(t, options)));
    return;
  }
  if (!(type in SPEC)) throw new Error(`Unknown type: ${type}. Use: skills, sessions, code, all`);
  await startConsumer(type as ConsumerType, options);
}

async function startConsumer(type: ConsumerType, options: ConsumeCommandOptions): Promise<void> {
  const s = SPEC[type];

  if (options.pubsub) {
    const config: PubSubConsumerConfig = { projectId: options.pubsub, inputTopic: s.input, outputTopic: s.output, subscriptionName: s.sub, dryRun: options.dryRun };
    const ConsumerCls = type === 'skills' ? PubSubSkillsConsumer : type === 'sessions' ? PubSubSessionsConsumer : PubSubCodeConsumer;
    const consumer = new ConsumerCls(config);
    await consumer.start();
    return;
  }

  const kafkaConfig = buildKafkaConfig(options.brokers);
  const consumerConfig: ConsumerConfig = {
    groupId: options.groupId || s.defaultGroup,
    inputTopic: s.kafkaInput,
    outputTopic: s.kafkaOutput,
    dlqTopic: s.kafkaDlq,
    batchSize: options.batchSize,
    fromBeginning: options.fromBeginning,
    dryRun: options.dryRun,
  };
  const ConsumerCls = type === 'skills' ? SkillsConsumer : type === 'sessions' ? SessionsConsumer : CodeConsumer;
  const consumer = new ConsumerCls(kafkaConfig, consumerConfig);
  await consumer.start();
}
