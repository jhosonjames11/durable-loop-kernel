declare module 'node:fs/promises' {
  export function mkdir(path: string, options?: { recursive?: boolean }): Promise<void>;
}
