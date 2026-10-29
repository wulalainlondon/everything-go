export async function sha256(bytes: Uint8Array): Promise<string> {
  const copy = Uint8Array.from(bytes);
  const value = await globalThis.crypto.subtle.digest('SHA-256', copy);
  return Array.from(new Uint8Array(value), x => x.toString(16).padStart(2, '0')).join('');
}

export function canonicalJSON(value: unknown): string {
  const string = (v: string): string => {
    for (let i = 0; i < v.length; i++) {
      const code = v.charCodeAt(i);
      if (code >= 0xd800 && code <= 0xdbff) {
        const next = v.charCodeAt(++i);
        if (!(next >= 0xdc00 && next <= 0xdfff)) throw new Error('Unpaired surrogate');
      } else if (code >= 0xdc00 && code <= 0xdfff) throw new Error('Unpaired surrogate');
    }
    return JSON.stringify(v).replace(/[<>&\u2028\u2029]/g,
      character => '\\u' + character.charCodeAt(0).toString(16).padStart(4, '0'));
  };
  const compare = (a: string, b: string): number => {
    const aa = Array.from(a, c => c.codePointAt(0)!);
    const bb = Array.from(b, c => c.codePointAt(0)!);
    for (let i = 0; i < Math.min(aa.length, bb.length); i++) if (aa[i] !== bb[i]) return aa[i]! - bb[i]!;
    return aa.length - bb.length;
  };
  const encode = (v: unknown): string => {
    if (typeof v === 'string') return string(v);
    if (v === null || typeof v === 'boolean') return JSON.stringify(v);
    if (typeof v === 'number' && Number.isSafeInteger(v)) return JSON.stringify(v);
    if (Array.isArray(v)) return '[' + v.map(encode).join(',') + ']';
    if (v && typeof v === 'object' &&
        (Object.getPrototypeOf(v) === Object.prototype || Object.getPrototypeOf(v) === null)) {
      return '{' + Object.keys(v).sort(compare).map(key => string(key) + ':' + encode((v as Record<string, unknown>)[key])).join(',') + '}';
    }
    throw new Error('Noncanonical JSON value');
  };
  return encode(value);
}

// Exact Go struct order: operation, input. Sorted input map and Go HTML escaping.
// Correlation and idempotency key are excluded; principal/generation remain the
// server's journal namespace, never an SDK grant or client-supplied role.
export async function inputHash(operation: string, input: unknown): Promise<string> {
  const operationJSON = canonicalJSON(operation);
  return sha256(new TextEncoder().encode('{"operation":' + operationJSON + ',"input":' + canonicalJSON(input) + '}'));
}

export function decodeBase64(value: string): Uint8Array {
  if (typeof value !== 'string' || !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)) {
    throw new Error('Noncanonical base64');
  }
  const raw = globalThis.atob(value);
  if (globalThis.btoa(raw) !== value) throw new Error('Noncanonical base64 padding bits');
  return Uint8Array.from(raw, character => character.charCodeAt(0));
}
