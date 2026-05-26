export interface SessionMessage {
  type: 'user' | 'assistant';
  timestamp: string | null;
  uuid: string | null;
  parentUuid: string | null;
  cwd: string | null;
  gitBranch: string | null;
  version: string | null;
  content: string;
  toolUse: ToolUseEntry[];
  tokenUsage: TokenUsage | null;
  model: string | null;
}

export interface ToolUseEntry {
  toolName: string;
  toolId: string;
  input: Record<string, unknown>;
}

export interface TokenUsage {
  inputTokens: number;
  outputTokens: number;
  cacheCreation: number;
  cacheRead: number;
}

export interface SessionMetadataFile {
  pid: number;
  cwd: string;
  startedAt: string;
  version: string;
  kind: string;
  entrypoint: string;
  status: string;
}

export interface HistoryEntry {
  display: string;
  timestamp: string;
  project: string;
  sessionId: string;
}

export interface RawSession {
  sessionId: string;
  project: string;
  filePath: string;
  messageCount: number;
  userMessageCount: number;
  assistantMessageCount: number;
  summaryCount: number;
  summaries: string[];
  messages: SessionMessage[];
  firstTimestamp: string | null;
  lastTimestamp: string | null;
  fileSize: number;
  timestamp: number;
  metadata?: SessionMetadataFile;
  historyEntries: HistoryEntry[];
  todos: unknown[];
}
