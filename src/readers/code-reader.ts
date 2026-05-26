import fs from 'fs';
import path from 'path';
import logger from '../utils/logger';
import { detectLanguage, isSourceFile, isExcludedDir } from '../utils/file-filter';
import type { RawCodeFile } from '../types/code';

export interface CodeReaderOptions {
  include?: string[];
  exclude?: string[];
  maxFileSize?: number;
  limit?: number;
}

const MAX_FILE_SIZE_DEFAULT = 1_048_576; // 1MB

export class CodeReader {
  private rootDir: string;
  private projectName: string;
  private maxFileSize: number;
  private limit: number;
  private customExcludes: Set<string>;

  constructor(rootDir: string, options: CodeReaderOptions = {}) {
    this.rootDir = path.resolve(rootDir);
    this.projectName = path.basename(this.rootDir);
    this.maxFileSize = options.maxFileSize ?? MAX_FILE_SIZE_DEFAULT;
    this.limit = options.limit ?? 0;
    this.customExcludes = new Set(options.exclude || []);
  }

  async readAll(): Promise<RawCodeFile[]> {
    const files: RawCodeFile[] = [];
    this.walkDir(this.rootDir, files);
    logger.info(`Read ${files.length} code files from ${this.rootDir}`);
    return files;
  }

  private walkDir(dir: string, results: RawCodeFile[]) {
    if (this.limit > 0 && results.length >= this.limit) return;

    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }

    for (const entry of entries) {
      if (this.limit > 0 && results.length >= this.limit) return;

      const fullPath = path.join(dir, entry.name);

      if (entry.isDirectory()) {
        if (isExcludedDir(entry.name) || this.customExcludes.has(entry.name)) continue;
        this.walkDir(fullPath, results);
      } else if (entry.isFile()) {
        if (!isSourceFile(fullPath)) continue;

        let stat: fs.Stats;
        try {
          stat = fs.statSync(fullPath);
        } catch {
          continue;
        }

        if (stat.size > this.maxFileSize) {
          logger.debug(`Skipping large file: ${fullPath} (${stat.size} bytes)`);
          continue;
        }
        if (stat.size === 0) continue;

        try {
          const content = fs.readFileSync(fullPath, 'utf-8');
          if (this.isBinary(content)) {
            logger.debug(`Skipping binary file: ${fullPath}`);
            continue;
          }

          const relativePath = path.relative(this.rootDir, fullPath);
          results.push({
            relativePath,
            absolutePath: fullPath,
            fileName: entry.name,
            extension: path.extname(entry.name),
            language: detectLanguage(fullPath),
            content,
            sizeBytes: stat.size,
            lineCount: content.split('\n').length,
            projectName: this.projectName,
          });
        } catch (err) {
          logger.warn(`Failed to read ${fullPath}: ${(err as Error).message}`);
        }
      }
    }
  }

  private isBinary(content: string): boolean {
    const sample = content.substring(0, 8192);
    return sample.includes('\0');
  }
}
