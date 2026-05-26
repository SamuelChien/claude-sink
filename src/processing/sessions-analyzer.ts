import type { ConsumptionChunk, SessionMetadata } from '../types/chunk';
import type { RawSession, SessionMessage } from '../types/session';
import type { AnalyzedSession } from '../types/analyzed';

const SESSION_CATEGORIES: Record<string, string[]> = {
  debugging: ['debug', 'error', 'fix', 'bug', 'issue', 'broken', 'crash', 'traceback', 'stack trace', 'exception'],
  'feature-development': ['implement', 'add', 'create', 'build', 'feature', 'new', 'scaffold'],
  refactoring: ['refactor', 'rename', 'restructure', 'clean', 'simplify', 'extract', 'move'],
  testing: ['test', 'spec', 'coverage', 'assert', 'expect', 'mock', 'jest', 'pytest', 'vitest'],
  deployment: ['deploy', 'release', 'docker', 'kubernetes', 'ci', 'cd', 'pipeline', 'publish'],
  documentation: ['doc', 'readme', 'comment', 'changelog', 'jsdoc'],
  architecture: ['design', 'architect', 'pattern', 'structure', 'schema', 'migration'],
  'data-work': ['database', 'query', 'sql', 'migration', 'seed', 'etl', 'data'],
  security: ['auth', 'permission', 'token', 'encrypt', 'vulnerability', 'secret'],
  configuration: ['config', 'setup', 'install', 'env', 'setting', 'dotenv'],
  learning: ['how', 'what', 'explain', 'understand', 'learn', 'why'],
  'code-review': ['review', 'pr', 'pull request', 'approve', 'comment'],
  'dependency-management': ['upgrade', 'update', 'dependency', 'package', 'npm', 'pip', 'cargo'],
};

export function analyzeSession(chunk: ConsumptionChunk): AnalyzedSession {
  const session: RawSession = JSON.parse(chunk.content);
  const meta = chunk.metadata as SessionMetadata;

  const duration = analyzeDuration(session);
  const tokenUsage = analyzeTokens(session.messages);
  const toolUsage = analyzeTools(session.messages);
  const categories = classifySession(session);
  const complexity = assessComplexity(session, toolUsage);
  const commands = analyzeCommands(session.messages);
  const models = analyzeModels(session.messages);

  return {
    chunkId: chunk.chunkId,
    sourceId: chunk.sourceId,
    sourcePath: chunk.sourcePath,
    timestamp: chunk.timestamp,
    sessionId: meta.sessionId,
    project: meta.project,
    messageCount: meta.messageCount,
    userMessageCount: meta.userMessageCount,
    assistantMessageCount: meta.assistantMessageCount,
    analysis: { duration, tokenUsage, toolUsage, categories, complexity, commands, models },
    analyzedAt: Date.now(),
  };
}

function analyzeDuration(session: RawSession): { durationMs: number | null; durationMinutes: number | null; bucket: string } {
  if (!session.firstTimestamp || !session.lastTimestamp) {
    return { durationMs: null, durationMinutes: null, bucket: 'unknown' };
  }
  const ms = new Date(session.lastTimestamp).getTime() - new Date(session.firstTimestamp).getTime();
  const minutes = ms / 60_000;
  const bucket = minutes < 5 ? 'quick' : minutes < 15 ? 'short' : minutes < 60 ? 'medium' : minutes < 240 ? 'long' : 'marathon';
  return { durationMs: ms, durationMinutes: Math.round(minutes * 10) / 10, bucket };
}

function analyzeTokens(messages: SessionMessage[]) {
  let totalInput = 0, totalOutput = 0, totalCacheCreation = 0, totalCacheRead = 0;

  for (const msg of messages) {
    if (msg.tokenUsage) {
      totalInput += msg.tokenUsage.inputTokens;
      totalOutput += msg.tokenUsage.outputTokens;
      totalCacheCreation += msg.tokenUsage.cacheCreation;
      totalCacheRead += msg.tokenUsage.cacheRead;
    }
  }

  const totalTokens = totalInput + totalOutput;
  const cacheHitRate = (totalCacheRead + totalInput) > 0 ? totalCacheRead / (totalCacheRead + totalInput) : 0;

  return { totalInput, totalOutput, totalCacheCreation, totalCacheRead, totalTokens, cacheHitRate: Math.round(cacheHitRate * 1000) / 1000 };
}

function analyzeTools(messages: SessionMessage[]) {
  const toolCounts = new Map<string, number>();
  const fileOps = new Map<string, { reads: number; writes: number; edits: number }>();

  for (const msg of messages) {
    for (const tool of msg.toolUse) {
      toolCounts.set(tool.toolName, (toolCounts.get(tool.toolName) || 0) + 1);

      const filePath = (tool.input as Record<string, unknown>).file_path as string | undefined;
      if (filePath) {
        const ops = fileOps.get(filePath) || { reads: 0, writes: 0, edits: 0 };
        if (['Read', 'read_file'].includes(tool.toolName)) ops.reads++;
        else if (['Write', 'write_file'].includes(tool.toolName)) ops.writes++;
        else if (['Edit', 'edit_file'].includes(tool.toolName)) ops.edits++;
        fileOps.set(filePath, ops);
      }
    }
  }

  const totalToolCalls = [...toolCounts.values()].reduce((a, b) => a + b, 0);
  const tools = [...toolCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([name, count]) => ({ name, count }));

  const filesAccessed = [...fileOps.entries()]
    .sort((a, b) => (b[1].reads + b[1].writes + b[1].edits) - (a[1].reads + a[1].writes + a[1].edits))
    .slice(0, 50)
    .map(([path, ops]) => ({ path, ...ops }));

  return { totalToolCalls, uniqueTools: toolCounts.size, tools, filesAccessed };
}

function classifySession(session: RawSession): { primary: string; secondary: string | null } {
  const userText = session.messages
    .filter(m => m.type === 'user')
    .map(m => m.content)
    .join(' ')
    .toLowerCase();

  const toolText = session.messages
    .flatMap(m => m.toolUse.map(t => t.toolName))
    .join(' ')
    .toLowerCase();

  const combined = `${userText} ${toolText}`;
  const scores: Record<string, number> = {};

  for (const [cat, keywords] of Object.entries(SESSION_CATEGORIES)) {
    scores[cat] = keywords.filter(k => combined.includes(k)).length;
  }

  const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  return {
    primary: sorted[0][1] > 0 ? sorted[0][0] : 'unknown',
    secondary: sorted[1]?.[1] > 0 ? sorted[1][0] : null,
  };
}

function assessComplexity(session: RawSession, toolUsage: { totalToolCalls: number; uniqueTools: number; filesAccessed: { path: string }[] }): { level: string; factors: string[] } {
  const factors: string[] = [];
  let score = 0;

  if (session.messageCount > 50) { score += 2; factors.push('many-messages'); }
  else if (session.messageCount > 20) { score += 1; factors.push('moderate-messages'); }
  if (toolUsage.uniqueTools > 5) { score += 2; factors.push('tool-diversity'); }
  if (toolUsage.filesAccessed.length > 10) { score += 2; factors.push('many-files'); }
  else if (toolUsage.filesAccessed.length > 3) { score += 1; factors.push('multi-file'); }
  if (toolUsage.totalToolCalls > 50) { score += 1; factors.push('heavy-tool-usage'); }

  const level = score >= 5 ? 'advanced' : score >= 3 ? 'complex' : score >= 1 ? 'moderate' : 'simple';
  return { level, factors };
}

function analyzeCommands(messages: SessionMessage[]): { topBinaries: { binary: string; count: number }[]; hasTests: boolean; hasBuilds: boolean; errorCount: number } {
  const binaryCounts = new Map<string, number>();
  let hasTests = false, hasBuilds = false, errorCount = 0;

  for (const msg of messages) {
    for (const tool of msg.toolUse) {
      if (!['Bash', 'bash'].includes(tool.toolName)) continue;
      const cmd = ((tool.input as Record<string, unknown>).command as string) || '';
      const binary = cmd.trim().split(/\s+/)[0]?.replace(/^\.\//, '');
      if (binary) binaryCounts.set(binary, (binaryCounts.get(binary) || 0) + 1);
      if (/\b(test|jest|pytest|vitest|mocha|spec)\b/i.test(cmd)) hasTests = true;
      if (/\b(build|compile|tsc|webpack|vite|esbuild)\b/i.test(cmd)) hasBuilds = true;
    }
    if (msg.type === 'assistant' && /error|exception|traceback|failed/i.test(msg.content)) {
      errorCount++;
    }
  }

  const topBinaries = [...binaryCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 10)
    .map(([binary, count]) => ({ binary, count }));

  return { topBinaries, hasTests, hasBuilds, errorCount };
}

function analyzeModels(messages: SessionMessage[]): { models: { model: string; messageCount: number }[]; primaryModel: string | null } {
  const modelCounts = new Map<string, number>();
  for (const msg of messages) {
    if (msg.model) modelCounts.set(msg.model, (modelCounts.get(msg.model) || 0) + 1);
  }
  const models = [...modelCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([model, messageCount]) => ({ model, messageCount }));
  return { models, primaryModel: models[0]?.model || null };
}
