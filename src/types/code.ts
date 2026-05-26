export interface RawCodeFile {
  relativePath: string;
  absolutePath: string;
  fileName: string;
  extension: string;
  language: string;
  content: string;
  sizeBytes: number;
  lineCount: number;
  projectName: string;
}
