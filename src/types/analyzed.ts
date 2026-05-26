export interface AnalyzedSkill {
  chunkId: string;
  sourceId: string;
  sourcePath: string;
  timestamp: number;
  name: string;
  description: string;
  tags: string[];
  tier: string;
  author: string;
  version: string;
  sourceCollection: string;
  bodyLength: number;
  headingCount: number;
  codeBlockCount: number;
  linkCount: number;
  analysis: {
    categories: { primary: string; secondary: string | null; scores: Record<string, number> };
    entities: { technologies: string[]; tools: string[]; platforms: string[]; concepts: string[] };
    qualityScore: { score: number; breakdown: Record<string, number> };
    complexity: { level: 'basic' | 'intermediate' | 'advanced'; factors: string[] };
    keywords: { word: string; count: number }[];
  };
  analyzedAt: number;
}

export interface AnalyzedSession {
  chunkId: string;
  sourceId: string;
  sourcePath: string;
  timestamp: number;
  sessionId: string;
  project: string;
  messageCount: number;
  userMessageCount: number;
  assistantMessageCount: number;
  analysis: {
    duration: { durationMs: number | null; durationMinutes: number | null; bucket: string };
    tokenUsage: { totalInput: number; totalOutput: number; totalCacheCreation: number; totalCacheRead: number; totalTokens: number; cacheHitRate: number };
    toolUsage: { totalToolCalls: number; uniqueTools: number; tools: { name: string; count: number }[]; filesAccessed: { path: string; reads: number; writes: number; edits: number }[] };
    categories: { primary: string; secondary: string | null };
    complexity: { level: string; factors: string[] };
    commands: { topBinaries: { binary: string; count: number }[]; hasTests: boolean; hasBuilds: boolean; errorCount: number };
    models: { models: { model: string; messageCount: number }[]; primaryModel: string | null };
  };
  analyzedAt: number;
}

export interface AnalyzedCode {
  chunkId: string;
  sourceId: string;
  sourcePath: string;
  timestamp: number;
  fileName: string;
  extension: string;
  language: string;
  projectName: string;
  sizeBytes: number;
  analysis: {
    lines: { total: number; code: number; comment: number; blank: number; commentRatio: number };
    imports: { all: string[]; external: string[]; internal: string[]; count: number };
    complexity: { score: number; level: 'low' | 'medium' | 'high' | 'very-high'; decisionPoints: number };
    structure: { functionCount: number; classCount: number; exportCount: number };
    fileRole: 'source' | 'test' | 'config' | 'build' | 'documentation' | 'migration' | 'unknown';
    languageFamily: string;
  };
  analyzedAt: number;
}
