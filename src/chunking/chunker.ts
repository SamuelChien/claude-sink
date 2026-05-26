import { sha256 } from '../utils/hash';
import type { ConsumptionChunk, SkillMetadata, SessionMetadata, CodeMetadata } from '../types/chunk';
import type { RawSkill } from '../types/skill';
import type { RawSession } from '../types/session';
import type { RawCodeFile } from '../types/code';

const MAX_CHUNK_BYTES = 900_000;

export function chunkSkill(skill: RawSkill): ConsumptionChunk {
  const content = JSON.stringify(skill);
  const metadata: SkillMetadata = {
    type: 'skill',
    name: skill.name,
    description: skill.description,
    tags: skill.tags,
    category: skill.category,
    author: skill.author,
    platforms: skill.platforms,
    allowedTools: skill.allowedTools,
    tier: skill.tier,
    version: skill.version,
    headingCount: skill.headings.length,
    codeBlockCount: skill.codeBlocks.length,
    linkCount: skill.links.length,
    sourceCollection: skill.sourceCollection,
  };

  return {
    chunkId: sha256(`skill:${skill.id}`),
    sourceType: 'skill',
    sourceId: skill.id,
    sourcePath: skill.filePath,
    timestamp: Date.now(),
    content,
    contentLength: Buffer.byteLength(content),
    contentHash: sha256(content),
    metadata,
  };
}

export function chunkSession(session: RawSession): ConsumptionChunk[] {
  const metadata: SessionMetadata = {
    type: 'session',
    sessionId: session.sessionId,
    project: session.project,
    messageCount: session.messageCount,
    userMessageCount: session.userMessageCount,
    assistantMessageCount: session.assistantMessageCount,
    fileSize: session.fileSize,
    firstTimestamp: session.firstTimestamp,
    lastTimestamp: session.lastTimestamp,
    summaryCount: session.summaryCount,
    hasTodos: session.todos.length > 0,
    hasMetadata: session.metadata !== undefined,
  };

  const fullContent = JSON.stringify(session);

  if (Buffer.byteLength(fullContent) <= MAX_CHUNK_BYTES) {
    return [{
      chunkId: sha256(`session:${session.sessionId}`),
      sourceType: 'session',
      sourceId: session.sessionId,
      sourcePath: session.filePath,
      timestamp: Date.now(),
      content: fullContent,
      contentLength: Buffer.byteLength(fullContent),
      contentHash: sha256(fullContent),
      metadata,
    }];
  }

  const chunks: ConsumptionChunk[] = [];
  const messages = session.messages;
  let start = 0;
  let chunkIndex = 0;

  while (start < messages.length) {
    let end = start + 1;
    while (end < messages.length) {
      const slice = { ...session, messages: messages.slice(start, end + 1) };
      if (Buffer.byteLength(JSON.stringify(slice)) > MAX_CHUNK_BYTES) break;
      end++;
    }

    const slicedSession = { ...session, messages: messages.slice(start, end) };
    const content = JSON.stringify(slicedSession);

    chunks.push({
      chunkId: sha256(`session:${session.sessionId}:${chunkIndex}`),
      sourceType: 'session',
      sourceId: session.sessionId,
      sourcePath: session.filePath,
      timestamp: Date.now(),
      content,
      contentLength: Buffer.byteLength(content),
      contentHash: sha256(content),
      metadata,
    });

    start = end;
    chunkIndex++;
  }

  return chunks;
}

export function chunkCodeFile(file: RawCodeFile): ConsumptionChunk {
  const metadata: CodeMetadata = {
    type: 'code',
    relativePath: file.relativePath,
    fileName: file.fileName,
    extension: file.extension,
    language: file.language,
    sizeBytes: file.sizeBytes,
    lineCount: file.lineCount,
    projectName: file.projectName,
  };

  const content = JSON.stringify(file);
  return {
    chunkId: sha256(`code:${file.projectName}:${file.relativePath}`),
    sourceType: 'code',
    sourceId: file.relativePath,
    sourcePath: file.absolutePath,
    timestamp: Date.now(),
    content,
    contentLength: Buffer.byteLength(content),
    contentHash: sha256(content),
    metadata,
  };
}
