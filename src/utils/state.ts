import fs from 'fs';
import path from 'path';
import logger from './logger';

const STATE_DIR = path.join(process.env.HOME || '', '.claude-sink');
const STATE_FILE = path.join(STATE_DIR, 'produced-hashes.json');

interface StateData {
  [topic: string]: { [sourceId: string]: string };
}

let state: StateData | null = null;

function load(): StateData {
  if (state) return state;
  try {
    if (fs.existsSync(STATE_FILE)) {
      state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf-8'));
      return state!;
    }
  } catch { /* corrupt file, start fresh */ }
  state = {};
  return state;
}

function save(): void {
  if (!state) return;
  if (!fs.existsSync(STATE_DIR)) fs.mkdirSync(STATE_DIR, { recursive: true });
  fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

export function wasProduced(topic: string, sourceId: string, contentHash: string): boolean {
  const s = load();
  return s[topic]?.[sourceId] === contentHash;
}

export function markProduced(topic: string, sourceId: string, contentHash: string): void {
  const s = load();
  if (!s[topic]) s[topic] = {};
  s[topic][sourceId] = contentHash;
}

export function commitState(): void {
  save();
}

export function filterNew(topic: string, chunks: { sourceId: string; contentHash: string }[]): number[] {
  const newIndices: number[] = [];
  for (let i = 0; i < chunks.length; i++) {
    if (!wasProduced(topic, chunks[i].sourceId, chunks[i].contentHash)) {
      newIndices.push(i);
    }
  }
  return newIndices;
}

export function getStateStats(): { topics: number; entries: number } {
  const s = load();
  const topics = Object.keys(s).length;
  const entries = Object.values(s).reduce((sum, t) => sum + Object.keys(t).length, 0);
  return { topics, entries };
}
