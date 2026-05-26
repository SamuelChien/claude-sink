import type { ConsumptionChunk, SkillMetadata } from '../types/chunk';
import type { RawSkill } from '../types/skill';
import type { AnalyzedSkill } from '../types/analyzed';

const CATEGORIES: Record<string, string[]> = {
  security: ['security', 'vulnerability', 'penetration', 'forensic', 'malware', 'threat', 'attack', 'defense', 'hardening', 'compliance', 'audit', 'exploit', 'encryption', 'firewall'],
  devops: ['docker', 'kubernetes', 'ci/cd', 'cicd', 'deploy', 'terraform', 'ansible', 'helm', 'argocd', 'pipeline', 'container', 'infrastructure'],
  frontend: ['react', 'vue', 'angular', 'css', 'html', 'ui', 'component', 'tailwind', 'svelte', 'responsive', 'browser', 'dom'],
  backend: ['api', 'database', 'server', 'rest', 'graphql', 'microservice', 'kafka', 'redis', 'middleware', 'endpoint', 'authentication'],
  'ml-ai': ['machine learning', 'model', 'training', 'llm', 'prompt', 'agent', 'embedding', 'inference', 'neural', 'transformer', 'fine-tune'],
  cloud: ['aws', 'azure', 'gcp', 'cloud', 'serverless', 'lambda', 's3', 'ec2', 'cloud-native'],
  data: ['analytics', 'pipeline', 'etl', 'warehouse', 'bigquery', 'clickhouse', 'spark', 'data-engineering', 'sql'],
  testing: ['test', 'testing', 'jest', 'pytest', 'e2e', 'integration test', 'unit test', 'coverage', 'tdd', 'playwright'],
  architecture: ['design', 'pattern', 'microservice', 'monolith', 'system', 'ddd', 'cqrs', 'event-driven', 'hexagonal'],
  documentation: ['document', 'readme', 'docs', 'writing', 'technical writing', 'changelog', 'api-doc'],
  automation: ['script', 'workflow', 'cron', 'webhook', 'automation', 'orchestrat', 'n8n', 'zapier'],
  observability: ['monitoring', 'logging', 'metrics', 'tracing', 'alert', 'grafana', 'prometheus', 'datadog', 'sentry'],
  mobile: ['ios', 'android', 'react native', 'flutter', 'swift', 'kotlin', 'mobile', 'expo'],
};

const TECH_LIST = new Set([
  'typescript', 'javascript', 'python', 'rust', 'go', 'java', 'ruby', 'swift', 'kotlin', 'c++', 'c#', 'php', 'scala', 'elixir', 'haskell',
  'react', 'vue', 'angular', 'svelte', 'nextjs', 'next.js', 'nuxt', 'express', 'fastapi', 'django', 'flask', 'spring', 'rails', 'laravel',
  'node.js', 'nodejs', 'deno', 'bun',
  'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'helm', 'argocd', 'jenkins', 'github actions',
  'postgresql', 'postgres', 'mysql', 'mongodb', 'redis', 'elasticsearch', 'neo4j', 'sqlite', 'dynamodb', 'cassandra',
  'kafka', 'rabbitmq', 'nats', 'pulsar',
  'aws', 'gcp', 'azure', 'cloudflare', 'vercel', 'netlify',
  'graphql', 'grpc', 'rest', 'websocket',
  'tailwind', 'sass', 'less', 'styled-components',
  'jest', 'vitest', 'pytest', 'mocha', 'playwright', 'cypress',
  'git', 'github', 'gitlab', 'bitbucket',
  'nginx', 'caddy', 'traefik',
  'prometheus', 'grafana', 'datadog', 'sentry', 'opentelemetry',
]);

const STOP_WORDS = new Set([
  'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were',
  'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might', 'can',
  'this', 'that', 'these', 'those', 'it', 'its', 'they', 'them', 'their', 'we', 'you', 'your', 'he', 'she', 'his', 'her',
  'not', 'no', 'if', 'then', 'else', 'when', 'where', 'how', 'what', 'which', 'who', 'whom', 'all', 'each', 'every', 'any', 'some',
  'use', 'using', 'used', 'uses', 'also', 'like', 'just', 'more', 'than', 'very', 'about', 'into', 'over', 'after', 'before',
]);

export function analyzeSkill(chunk: ConsumptionChunk): AnalyzedSkill {
  const skill: RawSkill = JSON.parse(chunk.content);
  const meta = chunk.metadata as SkillMetadata;
  const text = `${skill.name} ${skill.description} ${skill.body}`.toLowerCase();

  const categories = classifyCategory(text, meta.tags);
  const entities = extractEntities(text, meta);
  const qualityScore = scoreQuality(skill, meta);
  const complexity = assessComplexity(skill, meta);
  const keywords = extractKeywords(skill.body);

  return {
    chunkId: chunk.chunkId,
    sourceId: chunk.sourceId,
    sourcePath: chunk.sourcePath,
    timestamp: chunk.timestamp,
    name: meta.name,
    description: meta.description,
    tags: meta.tags,
    tier: meta.tier,
    author: meta.author,
    version: meta.version,
    sourceCollection: meta.sourceCollection,
    bodyLength: skill.bodyLength,
    headingCount: meta.headingCount,
    codeBlockCount: meta.codeBlockCount,
    linkCount: meta.linkCount,
    analysis: { categories, entities, qualityScore, complexity, keywords },
    analyzedAt: Date.now(),
  };
}

function classifyCategory(text: string, tags: string[]): { primary: string; secondary: string | null; scores: Record<string, number> } {
  const tagArr = Array.isArray(tags) ? tags : [];
  const tagText = tagArr.join(' ').toLowerCase();
  const combined = `${text} ${tagText} ${tagText}`;

  const scores: Record<string, number> = {};
  for (const [cat, keywords] of Object.entries(CATEGORIES)) {
    scores[cat] = keywords.filter(k => combined.includes(k)).length;
  }

  const sorted = Object.entries(scores).sort((a, b) => b[1] - a[1]);
  const primary = sorted[0][1] > 0 ? sorted[0][0] : 'general';
  const secondary = sorted[1]?.[1] > 0 ? sorted[1][0] : null;
  return { primary, secondary, scores };
}

function extractEntities(text: string, meta: SkillMetadata): { technologies: string[]; tools: string[]; platforms: string[]; concepts: string[] } {
  const technologies = [...TECH_LIST].filter(t => text.includes(t));
  const tools = meta.allowedTools.length > 0 ? meta.allowedTools : [];
  const platforms = meta.platforms.length > 0 ? meta.platforms : [];

  const conceptPatterns = ['ddd', 'tdd', 'bdd', 'cqrs', 'event sourcing', 'microservices', 'monorepo', 'hexagonal', 'clean architecture', 'solid', 'dry', 'kiss', 'yagni', 'ci/cd', 'gitops', 'devops', 'devsecops', 'zero trust', 'rag', 'fine-tuning'];
  const concepts = conceptPatterns.filter(c => text.includes(c));

  return { technologies, tools, platforms, concepts };
}

function scoreQuality(skill: RawSkill, meta: SkillMetadata): { score: number; breakdown: Record<string, number> } {
  const breakdown: Record<string, number> = {};
  breakdown.description = meta.description.length > 20 ? 10 : meta.description.length > 0 ? 5 : 0;
  breakdown.tags = Math.min(meta.tags.length * 3, 10);
  breakdown.codeBlocks = Math.min(meta.codeBlockCount * 5, 15);
  breakdown.headings = Math.min(meta.headingCount * 4, 15);
  breakdown.bodyDepth = skill.bodyLength > 2000 ? 20 : skill.bodyLength > 500 ? 15 : skill.bodyLength > 200 ? 10 : 0;
  breakdown.tools = meta.allowedTools.length > 0 ? 10 : 0;
  breakdown.links = Math.min(meta.linkCount * 5, 10);
  breakdown.supportingFiles = skill.supportingFiles.length > 0 ? 10 : 0;
  const score = Object.values(breakdown).reduce((a, b) => a + b, 0);
  return { score: Math.min(score, 100), breakdown };
}

function assessComplexity(skill: RawSkill, meta: SkillMetadata): { level: 'basic' | 'intermediate' | 'advanced'; factors: string[] } {
  const factors: string[] = [];
  let score = 0;

  if (meta.codeBlockCount >= 3) { score += 2; factors.push('multiple-code-blocks'); }
  if (meta.headingCount >= 5) { score += 2; factors.push('deep-structure'); }
  if (skill.bodyLength > 3000) { score += 2; factors.push('extensive-content'); }
  if (meta.allowedTools.length >= 5) { score += 1; factors.push('many-tools'); }
  if (meta.linkCount >= 3) { score += 1; factors.push('cross-references'); }

  const level = score >= 5 ? 'advanced' : score >= 2 ? 'intermediate' : 'basic';
  return { level, factors };
}

function extractKeywords(body: string): { word: string; count: number }[] {
  const words = body.toLowerCase().replace(/[^a-z0-9\s-]/g, '').split(/\s+/);
  const freq = new Map<string, number>();

  for (const w of words) {
    if (w.length < 3 || STOP_WORDS.has(w)) continue;
    freq.set(w, (freq.get(w) || 0) + 1);
  }

  return [...freq.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20)
    .map(([word, count]) => ({ word, count }));
}
