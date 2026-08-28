declare module 'node:sqlite' {
  export interface StatementSync {
    run(...parameters: unknown[]): { changes: number; lastInsertRowid: number | bigint };
    get(...parameters: unknown[]): unknown;
    all(...parameters: unknown[]): unknown[];
  }
  export class DatabaseSync {
    constructor(path: string);
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}

declare module 'node:crypto' {
  export function createHash(algorithm: string): {
    update(value: Uint8Array | string): { digest(encoding: 'hex'): string };
  };
  export function randomUUID(): string;
}

declare module 'node:fs' {
  export const constants: Record<string, number>;
  export function lstatSync(path: string): {
    isDirectory(): boolean;
    isFile(): boolean;
    isSymbolicLink(): boolean;
  };
}

declare module 'node:fs/promises' {
  interface FileHandle {
    readFile(): Promise<Uint8Array>;
    writeFile(data: Uint8Array | string): Promise<void>;
    sync(): Promise<void>;
    close(): Promise<void>;
  }
  interface Stats {
    mtimeMs: number;
    isDirectory(): boolean;
    isFile(): boolean;
    isSymbolicLink(): boolean;
  }
  interface Dirent {
    name: string;
    isSymbolicLink(): boolean;
  }
  export function mkdir(path: string, options?: { recursive?: boolean }): Promise<void>;
  export function lstat(path: string): Promise<Stats>;
  export function open(path: string, flags: string | number, mode?: number): Promise<FileHandle>;
  export function readdir(path: string, options: { withFileTypes: true }): Promise<Dirent[]>;
  export function readFile(path: string): Promise<Uint8Array>;
  export function writeFile(path: string, data: Uint8Array | string, options?: { flag?: string }): Promise<void>;
  export function link(existingPath: string, newPath: string): Promise<void>;
  export function rename(oldPath: string, newPath: string): Promise<void>;
  export function unlink(path: string): Promise<void>;
}

declare module 'node:path' {
  export function join(...paths: string[]): string;
}
