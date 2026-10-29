import {isMutation, type IntentJournal, type MutationOperation, type PendingIntent} from './client.ts';
import { canonicalJSON, inputHash } from './hash.ts';
import { parseStrict, validate, validateRequest } from './runtime.ts';

export interface StorageLike {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  key(index: number): string | null;
  readonly length: number;
}
export interface AtomicLock {
  // The host MUST serialize every participant for this storage namespace.
  // Browser hosts can pass navigator.locks. No unlocked fallback is provided.
  request<T>(name: string, operation: () => Promise<T>): Promise<T>;
}

export class StorageIntentJournal implements IntentJournal {
  private storage: StorageLike;
  private locks: AtomicLock;
  private prefix: string;
  private namespace: string;
  constructor(storage: StorageLike, locks: AtomicLock, namespace: string) {
    if (!namespace.trim() || !locks?.request) throw new Error('Actual namespace and cross-context lock required');
    this.storage = storage; this.locks = locks; this.namespace = namespace;
    this.prefix = 'bridge-controller-results-rc9:' + encodeURIComponent(namespace) + ':';
  }
  private key(operation: string, id: string): string {
    return this.prefix + encodeURIComponent(operation) + ':' + encodeURIComponent(id);
  }
  private async check(intent: PendingIntent): Promise<void> {
    validateRequest(intent.request);
    if (!isMutation(intent.request.operation) || !Number.isSafeInteger(intent.namespaceGeneration) || intent.namespaceGeneration < 0 || intent.namespace !== this.namespace || !intent.authority || !intent.request.idempotency_key ||
        !['prepared', 'unknown', 'resolved'].includes(intent.state) ||
        intent.inputHash !== await inputHash(intent.request.operation, intent.request.input)) throw new Error('Corrupt intent journal entry');
    if(intent.receipt)validate(intent.receipt,'Receipt');
    if (intent.receipt && (intent.receipt.intent_id !== intent.request.idempotency_key || intent.receipt.input_hash !== intent.inputHash || intent.receipt.operation !== intent.request.operation)) {
      throw new Error('Corrupt original receipt');
    }
  }
  private async load(key: string): Promise<PendingIntent | undefined> {
    const raw = this.storage.getItem(key);
    if (raw === null) return undefined;
    const value = parseStrict(raw, 16 * 1024 * 1024) as PendingIntent;
    await this.check(value);
    if (key !== this.key(value.request.operation, value.request.idempotency_key!)) throw new Error('Journal key/operation mismatch');
    return value;
  }
  async find(operation: MutationOperation, key: string): Promise<PendingIntent | undefined> {
    return this.load(this.key(operation, key));
  }
  async save(intent: PendingIntent): Promise<void> {
    intent = structuredClone(intent); await this.check(intent);
    const key = this.key(intent.request.operation, intent.request.idempotency_key!);
    await this.locks.request(key, async () => {
      const previous = await this.load(key);
      if (previous) {
        if (previous.authority !== intent.authority || previous.namespaceGeneration !== intent.namespaceGeneration || previous.inputHash !== intent.inputHash ||
            canonicalJSON(previous.request) !== canonicalJSON(intent.request)) throw new Error('Immutable original intent conflict');
        if (previous.state === 'resolved' && canonicalJSON(previous) !== canonicalJSON(intent)) throw new Error('Resolved original intent is immutable');
        if (previous.state === 'unknown' && intent.state === 'prepared') throw new Error('Unknown intent cannot be prepared again');
      } else if (intent.state !== 'prepared') throw new Error('Original prepared intent missing');
      this.storage.setItem(key, canonicalJSON(intent));
    });
  }
  async claim(intent: PendingIntent): Promise<boolean> {
    intent = structuredClone(intent); await this.check(intent);
    const key = this.key(intent.request.operation, intent.request.idempotency_key!);
    return this.locks.request(key, async () => {
      const previous = await this.load(key);
      if (!previous || previous.state !== 'prepared') return false;
      if (previous.authority !== intent.authority || previous.namespaceGeneration !== intent.namespaceGeneration || canonicalJSON(previous.request) !== canonicalJSON(intent.request)) throw new Error('Original claim identity conflict');
      this.storage.setItem(key, canonicalJSON({ ...previous, state: 'unknown' }));
      return true;
    });
  }
  async pending(): Promise<PendingIntent[]> {
    const values = [];
    for (let i = 0; i < this.storage.length; i++) {
      const key = this.storage.key(i);
      if (key?.startsWith(this.prefix)) {
        const intent = await this.load(key);
        if (intent && intent.state !== 'resolved') values.push(intent);
      }
    }
    return values;
  }
}
