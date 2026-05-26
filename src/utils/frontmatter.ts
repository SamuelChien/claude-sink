import yaml from 'js-yaml';

export interface ParsedFrontmatter {
  frontmatter: Record<string, unknown>;
  body: string;
}

export function extractFrontmatter(content: string): ParsedFrontmatter {
  const match = content.match(/^---\n([\s\S]*?)\n---\n([\s\S]*)$/);
  if (!match) return { frontmatter: {}, body: content };

  try {
    const frontmatter = (yaml.load(match[1]) as Record<string, unknown>) || {};
    return { frontmatter, body: match[2].trim() };
  } catch {
    return { frontmatter: {}, body: content };
  }
}
