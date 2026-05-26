import { SinkConsumer, ProcessingResult } from '../kafka/consumer';
import { analyzeSkill } from '../processing/skills-analyzer';
import type { ConsumptionChunk } from '../types/chunk';
import type { AnalyzedSkill } from '../types/analyzed';

export class SkillsConsumer extends SinkConsumer<AnalyzedSkill> {
  get name() { return 'skills-consumer'; }

  process(batch: ConsumptionChunk[]): ProcessingResult<AnalyzedSkill>[] {
    return batch.map(chunk => ({
      key: chunk.sourceId,
      value: analyzeSkill(chunk),
    }));
  }
}
