import { SkillsConsumer } from '../consumers/skills-consumer';
import { SessionsConsumer } from '../consumers/sessions-consumer';
import { CodeConsumer } from '../consumers/code-consumer';
import { buildKafkaConfig } from '../kafka/config';
import type { ConsumerConfig } from '../kafka/config';
import logger from '../utils/logger';

export interface ConsumeCommandOptions {
  brokers: string;
  groupId?: string;
  batchSize: number;
  fromBeginning: boolean;
  dryRun: boolean;
}

type ConsumerType = 'skills' | 'sessions' | 'code';

const CONSUMER_MAP = {
  skills: { inputTopic: 'sink.skills', outputTopic: 'sink.skills.analyzed', dlqTopic: 'sink.skills.dlq', defaultGroup: 'sink-skills-processor', Cls: SkillsConsumer },
  sessions: { inputTopic: 'sink.sessions', outputTopic: 'sink.sessions.analyzed', dlqTopic: 'sink.sessions.dlq', defaultGroup: 'sink-sessions-processor', Cls: SessionsConsumer },
  code: { inputTopic: 'sink.code', outputTopic: 'sink.code.analyzed', dlqTopic: 'sink.code.dlq', defaultGroup: 'sink-code-processor', Cls: CodeConsumer },
} as const;

export async function runConsume(type: string, options: ConsumeCommandOptions): Promise<void> {
  const kafkaConfig = buildKafkaConfig(options.brokers);

  if (type === 'all') {
    logger.info('Starting all consumers...');
    const promises = (['skills', 'sessions', 'code'] as ConsumerType[]).map(t =>
      startConsumer(t, kafkaConfig, options)
    );
    await Promise.all(promises);
    return;
  }

  if (!(type in CONSUMER_MAP)) {
    throw new Error(`Unknown consumer type: ${type}. Use: skills, sessions, code, all`);
  }

  await startConsumer(type as ConsumerType, kafkaConfig, options);
}

async function startConsumer(type: ConsumerType, kafkaConfig: ReturnType<typeof buildKafkaConfig>, options: ConsumeCommandOptions): Promise<void> {
  const spec = CONSUMER_MAP[type];
  const consumerConfig: ConsumerConfig = {
    groupId: options.groupId || spec.defaultGroup,
    inputTopic: spec.inputTopic,
    outputTopic: spec.outputTopic,
    dlqTopic: spec.dlqTopic,
    batchSize: options.batchSize,
    fromBeginning: options.fromBeginning,
    dryRun: options.dryRun,
  };

  const consumer = new spec.Cls(kafkaConfig, consumerConfig);
  await consumer.start();
}
