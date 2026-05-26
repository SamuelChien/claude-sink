export interface RawSkill {
  id: string;
  name: string;
  sourceCollection: string;
  description: string;
  tags: string[];
  category: string;
  tier: string;
  author: string;
  platforms: string[];
  allowedTools: string[];
  risk: string;
  version: string;
  body: string;
  bodyLength: number;
  headings: { level: number; text: string }[];
  links: { text: string; url: string }[];
  codeBlocks: { language: string; length: number }[];
  supportingFiles: string[];
  filePath: string;
  timestamp: number;
}
