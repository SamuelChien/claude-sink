import { PubSubConsumer, ProcessingResult } from './consumer';
import { analyzeSkill } from '../processing/skills-analyzer';
import { analyzeSession } from '../processing/sessions-analyzer';
import { analyzeCode } from '../processing/code-analyzer';
import type { ConsumptionChunk } from '../types/chunk';
import type { AnalyzedSkill } from '../types/analyzed';
import type { AnalyzedSession } from '../types/analyzed';
import type { AnalyzedCode } from '../types/analyzed';

export class PubSubSkillsConsumer extends PubSubConsumer<AnalyzedSkill> {
  get name() { return 'skills-consumer'; }
  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedSkill>[] {
    return batch.map(chunk => ({ key: chunk.sourceId, value: analyzeSkill(chunk) }));
  }
}

export class PubSubSessionsConsumer extends PubSubConsumer<AnalyzedSession> {
  get name() { return 'sessions-consumer'; }
  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedSession>[] {
    return batch.map(chunk => ({ key: chunk.sourceId, value: analyzeSession(chunk) }));
  }
}

export class PubSubCodeConsumer extends PubSubConsumer<AnalyzedCode> {
  get name() { return 'code-consumer'; }
  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedCode>[] {
    return batch.map(chunk => ({ key: chunk.sourceId, value: analyzeCode(chunk) }));
  }
}
