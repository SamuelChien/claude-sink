import path from 'path';
import { runSkills } from './skills';
import { runSessions } from './sessions';
import { runCode } from './code';
import logger from '../utils/logger';

export interface AllCommandOptions {
  brokers: string;
  batchSize: number;
  dryRun: boolean;
  skillsDir?: string;
  codeDir?: string;
  sessionsDir?: string;
}

export async function runAll(options: AllCommandOptions): Promise<void> {
  const brokers = options.brokers;
  const batchSize = options.batchSize;
  const dryRun = options.dryRun;

  if (options.skillsDir) {
    logger.info('=== Skills ingestion ===');
    await runSkills(options.skillsDir, {
      brokers, batchSize, dryRun,
      topic: 'sink.skills',
      maxDepth: 5,
      limit: 0,
    });
  }

  logger.info('=== Sessions ingestion ===');
  await runSessions(options.sessionsDir, {
    brokers, batchSize, dryRun,
    topic: 'sink.sessions',
    limit: 0,
  });

  if (options.codeDir) {
    logger.info('=== Code ingestion ===');
    await runCode(options.codeDir, {
      brokers, batchSize, dryRun,
      topic: 'sink.code',
      maxFileSize: 1_048_576,
      limit: 0,
    });
  }

  logger.info('=== All ingestion complete ===');
}
