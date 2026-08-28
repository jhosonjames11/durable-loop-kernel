import { createHash, randomUUID } from 'node:crypto';
import { constants, lstatSync } from 'node:fs';
import { link, lstat, mkdir, open, readdir, unlink } from 'node:fs/promises';
import { join } from 'node:path';

import type { ArtifactReference, ArtifactStore, GenerationManifest, NamedArtifact } from '@loopgraph/core';

import { canonicalJson } from './canonical-json.js';
import { StorageError } from './storage-error.js';

const DIGEST = /^[a-f0-9]{64}$/u;
const SAFE_NAME = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/u;
const DEFAULT_MAX_ARTIFACT_BYTES = 64 * 1024 * 1024;
const DEFAULT_MAX_ARTIFACT_COUNT = 256;
const DEFAULT_MAX_MANIFEST_BYTES = 1024 * 1024;
/** Temp files are only ours after this age; fresh writer files are never removed. */
export const STALE_TEMP_AGE_MS = 60 * 60 * 1000;

export interface FileArtifactStoreOptions {
  readonly maxArtifactBytes?: number;
  readonly maxArtifactCount?: number;
  readonly maxManifestBytes?: number;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function errorCode(error: unknown): string | undefined {
  return isRecord(error) && typeof error.code === 'string' ? error.code : undefined;
}

function positiveLimit(value: number | undefined, fallback: number, name: string): number {
  const limit = value ?? fallback;
  if (!Number.isSafeInteger(limit) || limit <= 0) {
    throw new StorageError('INVALID_ARGUMENT', `${name} must be a positive safe integer`);
  }
  return limit;
}

function ioError(error: unknown, missingMessage: string, ioMessage: string): StorageError {
  if (error instanceof StorageError) return error;
  return errorCode(error) === 'ENOENT'
    ? new StorageError('NOT_FOUND', missingMessage)
    : new StorageError('STORAGE_IO', ioMessage);
}

function safeGenerationId(generationId: string): void {
  if (!SAFE_NAME.test(generationId) || generationId === '.' || generationId === '..' || generationId.includes('..')) {
    throw new StorageError('INVALID_ARGUMENT', 'generation ID must be a safe filename');
  }
}

function safeArtifactName(name: string): void {
  if (!SAFE_NAME.test(name) || name === '.' || name === '..' || name.includes('..')) {
    throw new StorageError('INVALID_ARGUMENT', 'artifact name must be a safe artifact name');
  }
}

/** Filesystem content-addressed store; publication returns only after file and directory sync. */
export class FileArtifactStore implements ArtifactStore {
  readonly #root: string;
  readonly #blobs: string;
  readonly #manifests: string;
  readonly #maxArtifactBytes: number;
  readonly #maxArtifactCount: number;
  readonly #maxManifestBytes: number;
  readonly #ready: Promise<void>;

  constructor(rootDirectory: string, options: FileArtifactStoreOptions = {}) {
    this.#root = rootDirectory;
    this.#blobs = join(rootDirectory, 'blobs');
    this.#manifests = join(rootDirectory, 'manifests');
    this.#maxArtifactBytes = positiveLimit(options.maxArtifactBytes, DEFAULT_MAX_ARTIFACT_BYTES, 'maxArtifactBytes');
    this.#maxArtifactCount = positiveLimit(options.maxArtifactCount, DEFAULT_MAX_ARTIFACT_COUNT, 'maxArtifactCount');
    this.#maxManifestBytes = positiveLimit(options.maxManifestBytes, DEFAULT_MAX_MANIFEST_BYTES, 'maxManifestBytes');
    // A constructor cannot await, but pre-existing escapes fail synchronously and
    // the first operation awaits full private-root initialization/cleanup.
    this.#assertPreexistingSafeSync(this.#root);
    this.#assertPreexistingSafeSync(this.#blobs);
    this.#assertPreexistingSafeSync(this.#manifests);
    this.#ready = this.#initialize();
    // Initialization errors remain observable to every operation through
    // #ensureSafeDirectories(), but a bridge that never touches artifacts must
    // not create an unhandled rejection merely because it is torn down first.
    void this.#ready.catch(() => undefined);
  }

  async put(bytes: Uint8Array): Promise<ArtifactReference> {
    await this.#ensureSafeDirectories();
    const copy = new Uint8Array(bytes);
    if (copy.byteLength > this.#maxArtifactBytes) {
      throw new StorageError('INVALID_ARGUMENT', 'artifact exceeds the configured byte limit');
    }
    const reference = { digest: this.#digest(copy), byteSize: copy.byteLength };
    const blobPath = this.#blobPath(reference.digest);
    const temporaryPath = join(this.#blobs, `.blob-${randomUUID()}.tmp`);
    let published = false;
    try {
      await this.#writeSyncedTemporary(temporaryPath, copy);
      try {
        await this.#assertPathAbsentOrRegular(blobPath, 'artifact blob');
        await this.#link(temporaryPath, blobPath, 'artifact blob');
        published = true;
        await this.#syncDirectory(this.#blobs);
      } catch (error) {
        if (errorCode(error) !== 'EEXIST') throw error;
      }
      await this.get(reference);
    } catch (error) {
      throw ioError(error, 'artifact blob was not found', 'artifact write failed');
    } finally {
      await this.#removeTemporary(temporaryPath, published);
    }
    return reference;
  }

  async get(reference: ArtifactReference): Promise<Uint8Array> {
    await this.#ensureSafeDirectories();
    if (!this.#validReference(reference)) {
      throw new StorageError('INVALID_ARGUMENT', 'artifact reference is invalid');
    }
    let bytes: Uint8Array;
    try {
      bytes = await this.#readRegularFile(this.#blobPath(reference.digest), 'artifact blob');
    } catch (error) {
      throw ioError(error, 'artifact integrity check failed: blob is absent', 'artifact read failed');
    }
    if (bytes.byteLength !== reference.byteSize || this.#digest(bytes) !== reference.digest) {
      throw new StorageError('CORRUPT_EVENT', 'artifact integrity check failed: digest or byte size mismatch');
    }
    return new Uint8Array(bytes);
  }

  async publishGeneration(manifest: GenerationManifest): Promise<void> {
    await this.#ensureSafeDirectories();
    const checked = this.#validateManifest(manifest);
    for (const artifact of checked.artifacts) await this.get(artifact);
    const serialized = canonicalJson(checked);
    if (this.#byteLength(serialized) > this.#maxManifestBytes) {
      throw new StorageError('INVALID_ARGUMENT', 'manifest exceeds the configured byte limit');
    }
    const manifestPath = this.#manifestPath(checked.generationId);
    const temporaryPath = join(this.#manifests, `.manifest-${randomUUID()}.tmp`);
    let published = false;
    try {
      await this.#writeSyncedTemporary(temporaryPath, serialized);
      try {
        await this.#assertPathAbsentOrRegular(manifestPath, 'manifest');
        await this.#link(temporaryPath, manifestPath, 'manifest');
        published = true;
        // Directory fsync is required to claim the manifest commit marker is durable.
        await this.#syncDirectory(this.#manifests);
      } catch (error) {
        if (errorCode(error) !== 'EEXIST') throw error;
        const existing = await this.#readRawManifest(checked.generationId);
        if (new TextDecoder().decode(existing) !== serialized) {
          throw new StorageError('INVALID_ARGUMENT', 'generation manifest is already published with different content');
        }
      }
      // A manifest is never accepted as a complete publication without blobs.
      for (const artifact of checked.artifacts) await this.get(artifact);
    } catch (error) {
      throw ioError(error, 'manifest was not found', 'manifest publication failed');
    } finally {
      await this.#removeTemporary(temporaryPath, published);
    }
  }

  async readManifest(generationId: string): Promise<GenerationManifest> {
    await this.#ensureSafeDirectories();
    safeGenerationId(generationId);
    const raw = await this.#readRawManifest(generationId);
    if (raw.byteLength > this.#maxManifestBytes) {
      throw new StorageError('CORRUPT_EVENT', 'manifest exceeds the configured byte limit');
    }
    const text = new TextDecoder().decode(raw);
    let parsed: unknown;
    try { parsed = JSON.parse(text); } catch {
      throw new StorageError('CORRUPT_EVENT', 'manifest is malformed');
    }
    if (canonicalJson(parsed) !== text) throw new StorageError('CORRUPT_EVENT', 'manifest is not canonical');
    let manifest: GenerationManifest;
    try { manifest = this.#validateManifest(parsed); } catch (error) {
      if (error instanceof StorageError && error.code === 'INVALID_ARGUMENT') {
        throw new StorageError('CORRUPT_EVENT', 'manifest has invalid content');
      }
      throw error;
    }
    if (manifest.generationId !== generationId) {
      throw new StorageError('CORRUPT_EVENT', 'manifest generation ID does not match its commit path');
    }
    for (const artifact of manifest.artifacts) await this.get(artifact);
    return manifest;
  }

  #validReference(reference: ArtifactReference): boolean {
    return DIGEST.test(reference.digest) && Number.isSafeInteger(reference.byteSize)
      && reference.byteSize >= 0 && reference.byteSize <= this.#maxArtifactBytes;
  }

  #validateManifest(value: unknown): GenerationManifest {
    if (!isRecord(value) || typeof value.generationId !== 'string' || typeof value.createdAt !== 'string'
      || !Number.isFinite(Date.parse(value.createdAt)) || !Array.isArray(value.artifacts)
      || value.artifacts.length === 0 || value.artifacts.length > this.#maxArtifactCount) {
      throw new StorageError('INVALID_ARGUMENT', 'manifest is malformed or exceeds configured limits');
    }
    safeGenerationId(value.generationId);
    const names = new Set<string>();
    const digests = new Set<string>();
    const artifacts: NamedArtifact[] = [];
    for (const candidate of value.artifacts) {
      if (!isRecord(candidate) || typeof candidate.name !== 'string' || typeof candidate.digest !== 'string'
        || typeof candidate.byteSize !== 'number') {
        throw new StorageError('INVALID_ARGUMENT', 'manifest artifact is malformed');
      }
      safeArtifactName(candidate.name);
      const reference = { digest: candidate.digest, byteSize: candidate.byteSize };
      if (!this.#validReference(reference)) throw new StorageError('INVALID_ARGUMENT', 'manifest artifact reference is invalid');
      if (names.has(candidate.name)) throw new StorageError('INVALID_ARGUMENT', 'duplicate artifact name');
      if (digests.has(candidate.digest)) throw new StorageError('INVALID_ARGUMENT', 'duplicate artifact digest');
      names.add(candidate.name);
      digests.add(candidate.digest);
      artifacts.push({ name: candidate.name, ...reference });
    }
    return { generationId: value.generationId, createdAt: value.createdAt, artifacts };
  }

  #blobPath(digest: string): string {
    if (!DIGEST.test(digest)) throw new StorageError('INVALID_ARGUMENT', 'artifact digest is invalid');
    return join(this.#blobs, digest);
  }

  #manifestPath(generationId: string): string {
    safeGenerationId(generationId);
    return join(this.#manifests, `${generationId}.json`);
  }

  async #initialize(): Promise<void> {
    try {
      await mkdir(this.#root, { recursive: true });
      await this.#assertDirectory(this.#root, 'artifact root');
      await this.#ensureDirectory(this.#blobs, 'artifact blob directory');
      await this.#ensureDirectory(this.#manifests, 'artifact manifest directory');
      await this.#cleanupStaleTemps(this.#blobs, /^\.blob-[0-9a-f-]{36}\.tmp$/u);
      await this.#cleanupStaleTemps(this.#manifests, /^\.manifest-[0-9a-f-]{36}\.tmp$/u);
    } catch (error) {
      throw ioError(error, 'artifact root was not found', 'artifact store initialization failed');
    }
  }

  async #ensureSafeDirectories(): Promise<void> {
    await this.#ready;
    try {
      await this.#assertDirectory(this.#root, 'artifact root');
      await this.#assertDirectory(this.#blobs, 'artifact blob directory');
      await this.#assertDirectory(this.#manifests, 'artifact manifest directory');
    } catch (error) {
      throw ioError(error, 'artifact root was not found', 'artifact store directory access failed');
    }
  }

  #assertPreexistingSafeSync(path: string): void {
    try {
      const stat = lstatSync(path);
      if (stat.isSymbolicLink() || !stat.isDirectory()) {
        throw new StorageError('UNSAFE_FILESYSTEM', 'artifact store directory is not a real directory');
      }
    } catch (error) {
      if (error instanceof StorageError) throw error;
      if (errorCode(error) !== 'ENOENT') throw new StorageError('STORAGE_IO', 'artifact store directory inspection failed');
    }
  }

  async #ensureDirectory(path: string, label: string): Promise<void> {
    try { await lstat(path); } catch (error) {
      if (errorCode(error) !== 'ENOENT') throw error;
      await mkdir(path);
    }
    await this.#assertDirectory(path, label);
  }

  async #assertDirectory(path: string, label: string): Promise<void> {
    const stat = await lstat(path);
    if (stat.isSymbolicLink() || !stat.isDirectory()) {
      throw new StorageError('UNSAFE_FILESYSTEM', `${label} is not a real directory`);
    }
  }

  async #assertPathAbsentOrRegular(path: string, label: string): Promise<void> {
    try {
      const stat = await lstat(path);
      if (stat.isSymbolicLink() || !stat.isFile()) {
        throw new StorageError('UNSAFE_FILESYSTEM', `${label} is not a regular file`);
      }
    } catch (error) {
      if (errorCode(error) !== 'ENOENT') throw error;
    }
  }

  async #readRegularFile(path: string, label: string): Promise<Uint8Array> {
    const stat = await lstat(path);
    if (stat.isSymbolicLink() || !stat.isFile()) {
      throw new StorageError('UNSAFE_FILESYSTEM', `${label} is not a regular file`);
    }
    const handle = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
    try { return new Uint8Array(await handle.readFile()); } finally { await handle.close(); }
  }

  async #readRawManifest(generationId: string): Promise<Uint8Array> {
    try { return await this.#readRegularFile(this.#manifestPath(generationId), 'manifest'); } catch (error) {
      throw ioError(error, 'manifest was not found', 'manifest read failed');
    }
  }

  async #writeSyncedTemporary(path: string, content: Uint8Array | string): Promise<void> {
    let handle: Awaited<ReturnType<typeof open>> | undefined;
    try {
      handle = await open(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
      await handle.writeFile(content);
      await handle.sync();
    } catch (error) {
      throw ioError(error, 'temporary artifact was not found', 'temporary artifact write failed');
    } finally {
      if (handle !== undefined) await handle.close();
    }
  }

  async #link(source: string, destination: string, label: string): Promise<void> {
    try {
      await link(source, destination);
    } catch (error) {
      if (errorCode(error) === 'EEXIST') throw error;
      throw ioError(error, `${label} was not found`, `${label} publication failed`);
    }
  }

  async #syncDirectory(path: string): Promise<void> {
    let handle: Awaited<ReturnType<typeof open>> | undefined;
    try {
      handle = await open(path, constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
      await handle.sync();
    } catch (error) {
      // Some filesystems do not expose directory fsync. Returning success there
      // would falsely claim manifest/blob publication survived a power loss.
      if (['EINVAL', 'ENOTSUP', 'EOPNOTSUPP', 'EPERM', 'EISDIR'].includes(errorCode(error) ?? '')) {
        throw new StorageError('DURABILITY_UNAVAILABLE', 'filesystem does not support durable directory publication');
      }
      throw ioError(error, 'artifact directory was not found', 'artifact directory sync failed');
    } finally {
      if (handle !== undefined) await handle.close();
    }
  }

  async #removeTemporary(path: string, _published: boolean): Promise<void> {
    try { await unlink(path); } catch (error) {
      if (errorCode(error) !== 'ENOENT') throw ioError(error, 'temporary artifact was not found', 'temporary artifact cleanup failed');
    }
  }

  async #cleanupStaleTemps(directory: string, expression: RegExp): Promise<void> {
    const now = Date.now();
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      if (!expression.test(entry.name) || entry.isSymbolicLink()) continue;
      const path = join(directory, entry.name);
      const stat = await lstat(path);
      // Never follow a renamed/symlinked entry, and never touch fresh writer data.
      if (stat.isSymbolicLink() || !stat.isFile() || now - stat.mtimeMs < STALE_TEMP_AGE_MS) continue;
      await unlink(path);
    }
  }

  #digest(bytes: Uint8Array): string { return createHash('sha256').update(bytes).digest('hex'); }
  #byteLength(value: string): number { return new TextEncoder().encode(value).byteLength; }
}
