import type { ConsumptionChunk, CodeMetadata } from '../types/chunk';
import type { RawCodeFile } from '../types/code';
import type { AnalyzedCode } from '../types/analyzed';

const COMMENT_PATTERNS: Record<string, { line?: RegExp; blockStart?: RegExp; blockEnd?: RegExp }> = {
  typescript: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  javascript: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  python: { line: /^\s*#/, blockStart: /^\s*"""/, blockEnd: /"""/ },
  ruby: { line: /^\s*#/ },
  shell: { line: /^\s*#/ },
  go: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  rust: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  java: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  csharp: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  cpp: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  c: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  php: { line: /^\s*(\/\/|#)/, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  lua: { line: /^\s*--/ },
  sql: { line: /^\s*--/, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  yaml: { line: /^\s*#/ },
  toml: { line: /^\s*#/ },
  dockerfile: { line: /^\s*#/ },
  makefile: { line: /^\s*#/ },
  css: { blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  scss: { line: /^\s*\/\//, blockStart: /^\s*\/\*/, blockEnd: /\*\// },
  html: { blockStart: /^\s*<!--/, blockEnd: /-->/ },
  xml: { blockStart: /^\s*<!--/, blockEnd: /-->/ },
};

const IMPORT_PATTERNS: Record<string, RegExp[]> = {
  typescript: [/^\s*import\s+.*?from\s+['"](.+?)['"]/gm, /^\s*import\s+['"](.+?)['"]/gm, /require\(['"](.+?)['"]\)/gm],
  javascript: [/^\s*import\s+.*?from\s+['"](.+?)['"]/gm, /^\s*import\s+['"](.+?)['"]/gm, /require\(['"](.+?)['"]\)/gm],
  python: [/^\s*import\s+(\S+)/gm, /^\s*from\s+(\S+)\s+import/gm],
  go: [/^\s*"(.+?)"/gm],
  rust: [/^\s*use\s+(\S+?);/gm, /^\s*use\s+(\S+?)\s*\{/gm],
  java: [/^\s*import\s+(\S+?);/gm],
  ruby: [/^\s*require\s+['"](.+?)['"]/gm, /^\s*require_relative\s+['"](.+?)['"]/gm],
  php: [/^\s*use\s+(\S+?);/gm, /^\s*require(?:_once)?\s+['"](.+?)['"]/gm],
};

const LANG_FAMILIES: Record<string, string> = {
  typescript: 'ecmascript', javascript: 'ecmascript',
  python: 'python',
  go: 'go',
  rust: 'rust',
  java: 'jvm', kotlin: 'jvm', scala: 'jvm',
  ruby: 'ruby',
  swift: 'apple', dart: 'dart',
  c: 'c-family', cpp: 'c-family', csharp: 'dotnet',
  php: 'php',
  shell: 'shell',
  sql: 'query', graphql: 'query',
  html: 'markup', xml: 'markup', vue: 'markup', svelte: 'markup',
  css: 'style', scss: 'style', sass: 'style', less: 'style',
  yaml: 'config', toml: 'config', json: 'config',
  markdown: 'documentation',
  dockerfile: 'infrastructure', terraform: 'infrastructure',
  protobuf: 'schema',
};

export function analyzeCode(chunk: ConsumptionChunk): AnalyzedCode {
  const file: RawCodeFile = JSON.parse(chunk.content);
  const meta = chunk.metadata as CodeMetadata;

  const lines = analyzeLines(file.content, meta.language);
  const imports = extractImports(file.content, meta.language);
  const complexity = measureComplexity(file.content, file.lineCount);
  const structure = analyzeStructure(file.content, meta.language);
  const fileRole = detectFileRole(file.relativePath, file.fileName, meta.language);
  const languageFamily = LANG_FAMILIES[meta.language] || 'other';

  return {
    chunkId: chunk.chunkId,
    sourceId: chunk.sourceId,
    sourcePath: chunk.sourcePath,
    timestamp: chunk.timestamp,
    fileName: meta.fileName,
    extension: meta.extension,
    language: meta.language,
    projectName: meta.projectName,
    sizeBytes: meta.sizeBytes,
    analysis: { lines, imports, complexity, structure, fileRole, languageFamily },
    analyzedAt: Date.now(),
  };
}

function analyzeLines(content: string, language: string): { total: number; code: number; comment: number; blank: number; commentRatio: number } {
  const allLines = content.split('\n');
  const total = allLines.length;
  let comment = 0, blank = 0;
  const patterns = COMMENT_PATTERNS[language];
  let inBlock = false;

  for (const line of allLines) {
    if (line.trim() === '') { blank++; continue; }
    if (!patterns) continue;

    if (inBlock) {
      comment++;
      if (patterns.blockEnd?.test(line)) inBlock = false;
      continue;
    }
    if (patterns.line?.test(line)) { comment++; continue; }
    if (patterns.blockStart?.test(line)) {
      comment++;
      if (!patterns.blockEnd?.test(line)) inBlock = true;
    }
  }

  const code = total - comment - blank;
  const commentRatio = total > 0 ? Math.round((comment / total) * 1000) / 1000 : 0;
  return { total, code, comment, blank, commentRatio };
}

function extractImports(content: string, language: string): { all: string[]; external: string[]; internal: string[]; count: number } {
  const patterns = IMPORT_PATTERNS[language];
  if (!patterns) return { all: [], external: [], internal: [], count: 0 };

  const all = new Set<string>();
  for (const pattern of patterns) {
    const re = new RegExp(pattern.source, pattern.flags);
    let match;
    while ((match = re.exec(content)) !== null) {
      if (match[1]) all.add(match[1]);
    }
  }

  const allArr = [...all];
  const internal = allArr.filter(i => i.startsWith('.') || i.startsWith('/'));
  const external = allArr.filter(i => !i.startsWith('.') && !i.startsWith('/'));
  return { all: allArr, external, internal, count: allArr.length };
}

function measureComplexity(content: string, lineCount: number): { score: number; level: 'low' | 'medium' | 'high' | 'very-high'; decisionPoints: number } {
  const patterns = [/\bif\b/g, /\belse\b/g, /\bswitch\b/g, /\bcase\b/g, /\bfor\b/g, /\bwhile\b/g, /\bcatch\b/g, /&&/g, /\|\|/g, /\?[^?:]/g];
  let decisionPoints = 0;
  for (const p of patterns) {
    const matches = content.match(p);
    if (matches) decisionPoints += matches.length;
  }

  const score = lineCount > 0 ? Math.round((decisionPoints / lineCount) * 100) / 100 : 0;
  const level = score > 0.3 ? 'very-high' : score > 0.15 ? 'high' : score > 0.05 ? 'medium' : 'low';
  return { score, level, decisionPoints };
}

function analyzeStructure(content: string, language: string): { functionCount: number; classCount: number; exportCount: number } {
  let functionCount = 0, classCount = 0, exportCount = 0;

  if (['typescript', 'javascript'].includes(language)) {
    functionCount = (content.match(/\bfunction\s+\w+/g) || []).length + (content.match(/\b\w+\s*(?:=|:)\s*(?:async\s+)?\(/g) || []).length;
    classCount = (content.match(/\bclass\s+\w+/g) || []).length;
    exportCount = (content.match(/\bexport\s+/g) || []).length;
  } else if (language === 'python') {
    functionCount = (content.match(/^\s*def\s+/gm) || []).length;
    classCount = (content.match(/^\s*class\s+/gm) || []).length;
  } else if (language === 'go') {
    functionCount = (content.match(/^\s*func\s+/gm) || []).length;
  } else if (language === 'rust') {
    functionCount = (content.match(/^\s*(?:pub\s+)?fn\s+/gm) || []).length;
    classCount = (content.match(/^\s*(?:pub\s+)?struct\s+/gm) || []).length;
  } else if (language === 'java' || language === 'csharp') {
    functionCount = (content.match(/(?:public|private|protected|static)\s+\w+\s+\w+\s*\(/g) || []).length;
    classCount = (content.match(/\bclass\s+\w+/g) || []).length;
  }

  return { functionCount, classCount, exportCount };
}

function detectFileRole(relativePath: string, fileName: string, language: string): 'source' | 'test' | 'config' | 'build' | 'documentation' | 'migration' | 'unknown' {
  const lower = `${relativePath}/${fileName}`.toLowerCase();
  if (/\.(test|spec|_test)\.[^.]+$/.test(lower) || /\/__tests__\//.test(lower) || /\/test\//.test(lower)) return 'test';
  if (/migration|migrate/i.test(lower)) return 'migration';
  if (language === 'markdown' || language === 'documentation') return 'documentation';
  if (['yaml', 'toml', 'json'].includes(language) || /config|\.env|\.rc|tsconfig|package\.json|docker-compose/i.test(fileName)) return 'config';
  if (/dockerfile|makefile|webpack|vite\.config|rollup|esbuild/i.test(fileName) || language === 'dockerfile' || language === 'makefile') return 'build';
  return 'source';
}
