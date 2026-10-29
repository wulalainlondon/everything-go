// Reused strict parser from the immutable rc.1 SDK; rc.1 source remains unchanged.
// JSON.parse discards duplicate keys. This parser rejects them before compiling
// a command, including escaped aliases and duplicates inside arrays/objects.
export function parseStrict(source: string, maxBytes = 256 * 1024): unknown {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > 16 * 1024 * 1024) throw new Error('Invalid bounded JSON limit');
  if (new TextEncoder().encode(source).length > maxBytes) throw new Error('Frame too large');
  let at = 0;
  const space = () => { while (/[\t\r\n ]/.test(source[at] ?? '\0')) at++; };
  function string(): string {
    const start = at++;
    while (at < source.length) {
      const c = source[at++];
      if (c === '\\') { at++; continue; }
      if (c === '"') {
        const value = JSON.parse(source.slice(start, at)) as string;
        for (let i = 0; i < value.length; i++) {
          const n = value.charCodeAt(i);
          if (n >= 0xd800 && n <= 0xdbff) { const next = value.charCodeAt(++i); if (!(next >= 0xdc00 && next <= 0xdfff)) throw new Error('Unpaired surrogate'); }
          else if (n >= 0xdc00 && n <= 0xdfff) throw new Error('Unpaired surrogate');
        }
        return value;
      }
    }
    throw new Error('Unterminated string');
  }
  function value(depth: number): unknown {
    if (depth > 64) throw new Error('JSON nesting exceeds limit');
    space(); const c = source[at];
    if (c === '"') return string();
    if (c === '{') {
      at++; space(); const object: Record<string, unknown> = Object.create(null); const keys = new Set<string>();
      if (source[at] === '}') { at++; return object; }
      while (at < source.length) {
        space(); if (source[at] !== '"') throw new Error('Object key required');
        const key = string(); if (keys.has(key)) throw new Error('Duplicate JSON key'); keys.add(key);
        space(); if (source[at++] !== ':') throw new Error('Colon required'); object[key] = value(depth + 1);
        space(); const end = source[at++]; if (end === '}') return object; if (end !== ',') throw new Error('Comma required');
      }
      throw new Error('Unterminated object');
    }
    if (c === '[') {
      at++; space(); const array: unknown[] = []; if (source[at] === ']') { at++; return array; }
      while (at < source.length) { array.push(value(depth + 1)); space(); const end = source[at++]; if (end === ']') return array; if (end !== ',') throw new Error('Comma required'); }
      throw new Error('Unterminated array');
    }
    const start = at; while (at < source.length && !/[\t\r\n ,\]}]/.test(source[at]!)) at++;
    if (at === start) throw new Error('JSON value required');
    const token=source.slice(start,at);
    if(token!=='true'&&token!=='false'&&token!=='null'){
      if(!/^-?(0|[1-9][0-9]*)$/.test(token)||token.length>17)throw new Error('Noncanonical wire integer');
      const number=BigInt(token);if(number>9007199254740991n||number< -9007199254740991n)throw new Error('Unsafe wire integer');
    }
    return JSON.parse(token);
  }
  const result = value(0); space(); if (at !== source.length) throw new Error('Trailing JSON'); return result;
}
