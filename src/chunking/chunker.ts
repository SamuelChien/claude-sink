import { sha256 } from '../utils/hash';
import type { ConsumptionChunk, SkillMetadata, SessionMetadata, CodeMetadata } from '../types/chunk';
import type { RawSkill } from '../types/skill';
import type { RawSession } from '../types/session';
import type { RawCodeFile } from '../types/code';

const MAX_CHUNK_BYTES = 900_000;

export function chunkSkill(skill: RawSkill): ConsumptionChunk {
  const content = JSON.stringify({ ...skill, timestamp: undefined });
  const contentHash = sha256(content);
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
    chunkId: sha256(`skill:${skill.id}:${contentHash}`),
    sourceType: 'skill',
    sourceId: skill.id,
    sourcePath: skill.filePath,
    timestamp: Date.now(),
    content,
    contentLength: Buffer.byteLength(content),
    contentHash,
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

  const fullContent = JSON.stringify({ ...session, timestamp: undefined });
  const fullHash = sha256(fullContent);

  if (Buffer.byteLength(fullContent) <= MAX_CHUNK_BYTES) {
    return [{
      chunkId: sha256(`session:${session.sessionId}:${fullHash}`),
      sourceType: 'session',
      sourceId: session.sessionId,
      sourcePath: session.filePath,
      timestamp: Date.now(),
      content: fullContent,
      contentLength: Buffer.byteLength(fullContent),
      contentHash: fullHash,
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

    if (end === start + 1) {
      const singleCheck = { ...session, messages: messages.slice(start, start + 1) };
      if (Buffer.byteLength(JSON.stringify(singleCheck)) > MAX_CHUNK_BYTES) {
        end = start + 1;
      }
    }

    const slicedSession = { ...session, messages: messages.slice(start, Math.max(end, start + 1)) };
    const content = JSON.stringify(slicedSession);
    const contentHash = sha256(content);

    chunks.push({
      chunkId: sha256(`session:${session.sessionId}:${chunkIndex}:${contentHash}`),
      sourceType: 'session',
      sourceId: session.sessionId,
      sourcePath: session.filePath,
      timestamp: Date.now(),
      content,
      contentLength: Buffer.byteLength(content),
      contentHash,
      metadata,
    });

    start = Math.max(end, start + 1);
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
  const contentHash = sha256(content);
  return {
    chunkId: sha256(`code:${file.projectName}:${file.relativePath}:${contentHash}`),
    sourceType: 'code',
    sourceId: file.relativePath,
    sourcePath: file.absolutePath,
    timestamp: Date.now(),
    content,
    contentLength: Buffer.byteLength(content),
    contentHash,
    metadata,
  };
}
