declare module 'node:crypto' {
  export function createHash(algorithm: string): {
    update(value: string): { digest(encoding: 'hex'): string };
  };
  export function randomUUID(): string;
}

declare module 'node:fs' {
  export function existsSync(path: string): boolean;
}

declare module 'node:fs/promises' {
  export function mkdtemp(prefix: string): Promise<string>;
  export function rm(path: string, options: { recursive: boolean; force: boolean }): Promise<void>;
}

declare module 'node:os' {
  export function tmpdir(): string;
}

declare module 'node:path' {
  export function join(...paths: string[]): string;
}

declare const process: {
  argv: string[];
  exitCode?: number;
  stdout: { write(value: string): void };
};
