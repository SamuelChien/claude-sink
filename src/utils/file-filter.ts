import path from 'path';

const LANGUAGE_MAP: Record<string, string> = {
  '.ts': 'typescript', '.tsx': 'typescript',
  '.js': 'javascript', '.jsx': 'javascript', '.mjs': 'javascript', '.cjs': 'javascript',
  '.py': 'python',
  '.rs': 'rust',
  '.go': 'go',
  '.java': 'java',
  '.rb': 'ruby',
  '.swift': 'swift',
  '.kt': 'kotlin', '.kts': 'kotlin',
  '.c': 'c', '.h': 'c',
  '.cpp': 'cpp', '.cc': 'cpp', '.cxx': 'cpp', '.hpp': 'cpp',
  '.cs': 'csharp',
  '.php': 'php',
  '.sh': 'shell', '.bash': 'shell', '.zsh': 'shell',
  '.sql': 'sql',
  '.yaml': 'yaml', '.yml': 'yaml',
  '.json': 'json',
  '.toml': 'toml',
  '.md': 'markdown',
  '.html': 'html', '.htm': 'html',
  '.css': 'css',
  '.scss': 'scss', '.sass': 'sass', '.less': 'less',
  '.xml': 'xml',
  '.graphql': 'graphql', '.gql': 'graphql',
  '.proto': 'protobuf',
  '.tf': 'terraform',
  '.dockerfile': 'dockerfile',
  '.lua': 'lua',
  '.r': 'r',
  '.scala': 'scala',
  '.dart': 'dart',
  '.vue': 'vue',
  '.svelte': 'svelte',
};

const DEFAULT_EXCLUDE_DIRS = new Set([
  'node_modules', '.git', 'dist', 'build', '.next', '__pycache__',
  '.venv', 'venv', 'vendor', '.cache', '.turbo', 'coverage',
  '.svn', '.hg', 'target', 'out', 'bin', 'obj',
]);

export function detectLanguage(filePath: string): string {
  const ext = path.extname(filePath).toLowerCase();
  if (LANGUAGE_MAP[ext]) return LANGUAGE_MAP[ext];

  const base = path.basename(filePath).toLowerCase();
  if (base === 'dockerfile' || base.startsWith('dockerfile.')) return 'dockerfile';
  if (base === 'makefile') return 'makefile';

  return 'unknown';
}

const EXCLUDED_FILES = new Set([
  'package-lock.json', 'yarn.lock', 'pnpm-lock.yaml',
  'composer.lock', 'cargo.lock', 'gemfile.lock', 'poetry.lock',
]);

export function isSourceFile(filePath: string): boolean {
  const ext = path.extname(filePath).toLowerCase();
  const base = path.basename(filePath).toLowerCase();
  if (EXCLUDED_FILES.has(base)) return false;
  return LANGUAGE_MAP[ext] !== undefined || base === 'dockerfile' || base === 'makefile';
}

export function isExcludedDir(dirName: string): boolean {
  return dirName.startsWith('.') || DEFAULT_EXCLUDE_DIRS.has(dirName);
}

export function getSupportedExtensions(): string[] {
  return Object.keys(LANGUAGE_MAP);
}
