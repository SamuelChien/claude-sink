import fs from 'fs';
import path from 'path';
import logger from '../utils/logger';
import type { RawSession, SessionMessage, ToolUseEntry, TokenUsage, HistoryEntry, SessionMetadataFile } from '../types/session';

export interface SessionsReaderOptions {
  limit?: number;
  since?: string;
  project?: string;
}

export class SessionsReader {
  private claudeDir: string;
  private projectsDir: string;
  private sessionsDir: string;
  private todosDir: string;
  private historyFile: string;
  private limit: number;
  private since: string | null;
  private projectFilter: string | null;

  constructor(claudeDir?: string, options: SessionsReaderOptions = {}) {
    this.claudeDir = claudeDir || path.join(process.env.HOME || '', '.claude');
    this.projectsDir = path.join(this.claudeDir, 'projects');
    this.sessionsDir = path.join(this.claudeDir, 'sessions');
    this.todosDir = path.join(this.claudeDir, 'todos');
    this.historyFile = path.join(this.claudeDir, 'history.jsonl');
    this.limit = options.limit ?? 0;
    this.since = options.since ?? null;
    this.projectFilter = options.project ?? null;
  }

  async readAll(): Promise<RawSession[]> {
    const sessions: RawSession[] = [];
    const projectDirs = this.listProjectDirs();

    for (const projectDir of projectDirs) {
      const projectName = path.basename(projectDir);
      if (this.projectFilter && projectName !== this.projectFilter) continue;

      const jsonlFiles = fs.readdirSync(projectDir).filter(f => f.endsWith('.jsonl'));

      for (const file of jsonlFiles) {
        if (this.limit > 0 && sessions.length >= this.limit) break;
        try {
          const session = this.readSessionFile(path.join(projectDir, file), projectName);
          if (session && session.messages.length > 0) {
            if (this.since && session.lastTimestamp && session.lastTimestamp < this.since) continue;
            sessions.push(session);
          }
        } catch (err) {
          logger.warn(`Error reading session ${file}: ${(err as Error).message}`);
        }
      }
    }

    const metadata = this.readSessionMetadata();
    const history = this.readHistory();
    const todos = this.readTodos();

    for (const session of sessions) {
      const meta = metadata[session.sessionId];
      if (meta) session.metadata = meta;
      session.historyEntries = history.filter(h => h.sessionId === session.sessionId);
      if (todos[session.sessionId]) session.todos = todos[session.sessionId];
    }

    logger.info(`Read ${sessions.length} sessions across ${projectDirs.length} projects`);
    return sessions;
  }

  private listProjectDirs(): string[] {
    if (!fs.existsSync(this.projectsDir)) return [];
    return fs.readdirSync(this.projectsDir)
      .map(d => path.join(this.projectsDir, d))
      .filter(d => fs.statSync(d).isDirectory());
  }

  private readSessionFile(filePath: string, projectName: string): RawSession | null {
    const content = fs.readFileSync(filePath, 'utf-8').trim();
    if (!content) return null;

    const lines = content.split('\n');
    const messages: SessionMessage[] = [];
    const summaries: string[] = [];
    let sessionId = path.basename(filePath, '.jsonl');

    for (const line of lines) {
      try {
        const entry = JSON.parse(line);
        if (entry.type === 'summary') {
          summaries.push(entry.summary);
          continue;
        }
        if (entry.sessionId) sessionId = entry.sessionId;
        if (entry.type === 'user' || entry.type === 'assistant') {
          messages.push({
            type: entry.type,
            timestamp: entry.timestamp || null,
            uuid: entry.uuid || null,
            parentUuid: entry.parentUuid || null,
            cwd: entry.cwd || null,
            gitBranch: entry.gitBranch || null,
            version: entry.version || null,
            content: this.extractContent(entry),
            toolUse: this.extractToolUse(entry),
            tokenUsage: this.extractTokenUsage(entry),
            model: this.extractModel(entry),
          });
        }
      } catch {
        logger.debug(`Skipping malformed line in ${filePath}`);
      }
    }

    const firstUserMsg = messages.find(m => m.type === 'user');
    const lastMsg = messages[messages.length - 1];

    return {
      sessionId,
      project: projectName,
      filePath,
      messageCount: messages.length,
      userMessageCount: messages.filter(m => m.type === 'user').length,
      assistantMessageCount: messages.filter(m => m.type === 'assistant').length,
      summaryCount: summaries.length,
      summaries,
      messages,
      firstTimestamp: firstUserMsg?.timestamp || null,
      lastTimestamp: lastMsg?.timestamp || null,
      fileSize: fs.statSync(filePath).size,
      timestamp: Date.now(),
      historyEntries: [],
      todos: [],
    };
  }

  private extractContent(entry: Record<string, unknown>): string {
    const msg = entry.message as Record<string, unknown> | undefined;
    if (!msg) return '';
    if (typeof msg.content === 'string') return msg.content;
    if (Array.isArray(msg.content)) {
      return (msg.content as Array<Record<string, unknown>>)
        .filter(block => block.type === 'text')
        .map(block => block.text as string)
        .join('\n');
    }
    return '';
  }

  private extractToolUse(entry: Record<string, unknown>): ToolUseEntry[] {
    const msg = entry.message as Record<string, unknown> | undefined;
    if (!msg || !Array.isArray(msg.content)) return [];
    return (msg.content as Array<Record<string, unknown>>)
      .filter(block => block.type === 'tool_use')
      .map(block => ({
        toolName: block.name as string,
        toolId: block.id as string,
        input: this.summarizeToolInput(block.name as string, block.input as Record<string, unknown>),
      }));
  }

  private summarizeToolInput(toolName: string, input: Record<string, unknown> | undefined): Record<string, unknown> {
    if (!input) return {};
    if (['Read', 'read_file', 'Edit', 'edit_file', 'Write', 'write_file'].includes(toolName)) {
      return { file_path: input.file_path };
    }
    if (toolName === 'Bash' || toolName === 'bash') {
      const cmd = (input.command as string) || '';
      return { command: cmd.substring(0, 200) };
    }
    if (toolName === 'WebSearch' || toolName === 'web_search') {
      return { query: input.query };
    }
    if (toolName === 'WebFetch' || toolName === 'web_fetch') {
      return { url: input.url };
    }
    return { type: toolName };
  }

  private extractTokenUsage(entry: Record<string, unknown>): TokenUsage | null {
    const msg = entry.message as Record<string, unknown> | undefined;
    const usage = msg?.usage as Record<string, number> | undefined;
    if (!usage) return null;
    return {
      inputTokens: usage.input_tokens || 0,
      outputTokens: usage.output_tokens || 0,
      cacheCreation: usage.cache_creation_input_tokens || 0,
      cacheRead: usage.cache_read_input_tokens || 0,
    };
  }

  private extractModel(entry: Record<string, unknown>): string | null {
    const msg = entry.message as Record<string, unknown> | undefined;
    return (msg?.model as string) || null;
  }

  private readSessionMetadata(): Record<string, SessionMetadataFile> {
    const metadata: Record<string, SessionMetadataFile> = {};
    if (!fs.existsSync(this.sessionsDir)) return metadata;

    for (const file of fs.readdirSync(this.sessionsDir)) {
      if (!file.endsWith('.json')) continue;
      try {
        const data = JSON.parse(fs.readFileSync(path.join(this.sessionsDir, file), 'utf-8'));
        if (data.sessionId) {
          metadata[data.sessionId] = {
            pid: data.pid,
            cwd: data.cwd,
            startedAt: data.startedAt,
            version: data.version,
            kind: data.kind,
            entrypoint: data.entrypoint,
            status: data.status,
          };
        }
      } catch { /* ignore */ }
    }
    return metadata;
  }

  private readHistory(): HistoryEntry[] {
    if (!fs.existsSync(this.historyFile)) return [];
    const entries: HistoryEntry[] = [];
    const content = fs.readFileSync(this.historyFile, 'utf-8').trim();
    if (!content) return entries;

    for (const line of content.split('\n')) {
      try {
        const entry = JSON.parse(line);
        entries.push({
          display: entry.display,
          timestamp: entry.timestamp,
          project: entry.project,
          sessionId: entry.sessionId,
        });
      } catch { /* ignore */ }
    }
    return entries;
  }

  private readTodos(): Record<string, unknown[]> {
    const todos: Record<string, unknown[]> = {};
    if (!fs.existsSync(this.todosDir)) return todos;

    for (const file of fs.readdirSync(this.todosDir)) {
      if (!file.endsWith('.json')) continue;
      try {
        const data = JSON.parse(fs.readFileSync(path.join(this.todosDir, file), 'utf-8'));
        const sessionId = file.split('-agent-')[0];
        if (!todos[sessionId]) todos[sessionId] = [];
        todos[sessionId].push(data);
      } catch { /* ignore */ }
    }
    return todos;
  }
}
