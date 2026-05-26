export type SourceType = 'skill' | 'session' | 'code';

export interface ConsumptionChunk {
  chunkId: string;
  sourceType: SourceType;
  sourceId: string;
  sourcePath: string;
  timestamp: number;
  content: string;
  contentLength: number;
  contentHash: string;
  metadata: SkillMetadata | SessionMetadata | CodeMetadata;
}

export interface SkillMetadata {
  type: 'skill';
  name: string;
  description: string;
  tags: string[];
  category: string;
  author: string;
  platforms: string[];
  allowedTools: string[];
  tier: string;
  version: string;
  headingCount: number;
  codeBlockCount: number;
  linkCount: number;
  sourceCollection: string;
}

export interface SessionMetadata {
  type: 'session';
  sessionId: string;
  project: string;
  messageCount: number;
  userMessageCount: number;
  assistantMessageCount: number;
  fileSize: number;
  firstTimestamp: string | null;
  lastTimestamp: string | null;
  summaryCount: number;
  hasTodos: boolean;
  hasMetadata: boolean;
}

export interface CodeMetadata {
  type: 'code';
  relativePath: string;
  fileName: string;
  extension: string;
  language: string;
  sizeBytes: number;
  lineCount: number;
  projectName: string;
}
