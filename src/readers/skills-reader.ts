import fs from 'fs';
import path from 'path';
import { extractFrontmatter } from '../utils/frontmatter';
import logger from '../utils/logger';
import type { RawSkill } from '../types/skill';

export interface SkillsReaderOptions {
  maxDepth?: number;
  limit?: number;
}

export class SkillsReader {
  private skillsDir: string;
  private seenIds = new Set<string>();
  private maxDepth: number;
  private limit: number;

  constructor(skillsDir: string, options: SkillsReaderOptions = {}) {
    this.skillsDir = skillsDir;
    this.maxDepth = options.maxDepth ?? 5;
    this.limit = options.limit ?? 0;
  }

  async readAll(): Promise<RawSkill[]> {
    const skills: RawSkill[] = [];
    const topEntries = fs.readdirSync(this.skillsDir, { withFileTypes: true });

    for (const topEntry of topEntries) {
      if (this.limit > 0 && skills.length >= this.limit) break;
      if (!topEntry.isDirectory() || topEntry.name.startsWith('.')) continue;

      const sourceDir = path.join(this.skillsDir, topEntry.name);
      const directSkill = path.join(sourceDir, 'SKILL.md');

      if (fs.existsSync(directSkill)) {
        const skill = this.parseSkillFile(directSkill, topEntry.name, topEntry.name);
        if (skill && !this.seenIds.has(skill.id)) {
          this.seenIds.add(skill.id);
          skills.push(skill);
        }
        continue;
      }

      const found = this.findSkillFiles(sourceDir, topEntry.name, 0);
      for (const { filePath, dirName, source } of found) {
        if (this.limit > 0 && skills.length >= this.limit) break;
        const skill = this.parseSkillFile(filePath, dirName, source);
        if (skill && !this.seenIds.has(skill.id)) {
          this.seenIds.add(skill.id);
          skills.push(skill);
        }
      }
    }

    logger.info(`Read ${skills.length} skills from ${this.skillsDir}`);
    return skills;
  }

  private findSkillFiles(dir: string, source: string, depth: number): { filePath: string; dirName: string; source: string }[] {
    const results: { filePath: string; dirName: string; source: string }[] = [];
    if (depth > this.maxDepth) return results;

    let entries: fs.Dirent[];
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return results;
    }

    for (const entry of entries) {
      if (!entry.isDirectory() || entry.name.startsWith('.') || entry.name === 'node_modules') continue;
      const subDir = path.join(dir, entry.name);
      const skillFile = path.join(subDir, 'SKILL.md');

      if (fs.existsSync(skillFile)) {
        results.push({ filePath: skillFile, dirName: entry.name, source });
      } else {
        results.push(...this.findSkillFiles(subDir, source, depth + 1));
      }
    }

    return results;
  }

  private parseSkillFile(filePath: string, dirName: string, sourceCollection: string): RawSkill | null {
    try {
      const raw = fs.readFileSync(filePath, 'utf-8');
      const { frontmatter, body } = extractFrontmatter(raw);

      const fm = frontmatter as Record<string, unknown>;
      const headings = this.extractHeadings(body);
      const links = this.extractLinks(body);
      const codeBlocks = this.extractCodeBlocks(body);

      return {
        id: dirName,
        name: (fm.name as string) || dirName,
        sourceCollection,
        description: (fm.description as string) || this.extractFirstParagraph(body),
        tags: (fm.tags as string[]) || [],
        category: (fm.category as string) || 'general',
        tier: (fm.tier as string) || 'standard',
        author: (fm.author as string) || 'unknown',
        platforms: (fm.platforms as string[]) || [],
        allowedTools: (fm['allowed-tools'] as string[]) || (fm.allowedTools as string[]) || [],
        risk: (fm.risk as string) || 'safe',
        version: (fm.version as string) || '1.0.0',
        body,
        bodyLength: body.length,
        headings,
        links,
        codeBlocks,
        supportingFiles: this.findSupportingFiles(path.dirname(filePath)),
        filePath,
        timestamp: Date.now(),
      };
    } catch (err) {
      logger.warn(`Failed to parse skill ${dirName}: ${(err as Error).message}`);
      return null;
    }
  }

  private extractFirstParagraph(body: string): string {
    const lines = body.split('\n').filter(l => l.trim() && !l.startsWith('#'));
    return lines.slice(0, 3).join(' ').substring(0, 300);
  }

  private extractHeadings(body: string): { level: number; text: string }[] {
    const headings: { level: number; text: string }[] = [];
    const re = /^(#{1,6})\s+(.+)$/gm;
    let match;
    while ((match = re.exec(body)) !== null) {
      headings.push({ level: match[1].length, text: match[2].trim() });
    }
    return headings;
  }

  private extractLinks(body: string): { text: string; url: string }[] {
    const links: { text: string; url: string }[] = [];
    const re = /\[([^\]]+)\]\(([^)]+)\)/g;
    let match;
    while ((match = re.exec(body)) !== null) {
      links.push({ text: match[1], url: match[2] });
    }
    return links;
  }

  private extractCodeBlocks(body: string): { language: string; length: number }[] {
    const blocks: { language: string; length: number }[] = [];
    const re = /```(\w*)\n([\s\S]*?)```/g;
    let match;
    while ((match = re.exec(body)) !== null) {
      blocks.push({ language: match[1] || 'text', length: match[2].length });
    }
    return blocks;
  }

  private findSupportingFiles(skillDir: string): string[] {
    const files: string[] = [];
    try {
      const walk = (dir: string, prefix = '') => {
        const entries = fs.readdirSync(dir, { withFileTypes: true });
        for (const entry of entries) {
          if (entry.name === 'SKILL.md' || entry.name === 'node_modules') continue;
          const rel = prefix ? `${prefix}/${entry.name}` : entry.name;
          if (entry.isDirectory()) {
            walk(path.join(dir, entry.name), rel);
          } else {
            files.push(rel);
          }
        }
      };
      walk(skillDir);
    } catch { /* ignore */ }
    return files;
  }
}
