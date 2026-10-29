import { Ajv2020 } from 'ajv/dist/2020.js';
import type { ValidateFunction } from 'ajv';
import schema from './api-contract.schema.json' with { type: 'json' };
import { parseStrict } from './strict-json.ts';
import { canonicalJSON } from './hash.ts';
export { apiVersion, schemaHash } from './schema-metadata.ts';
export { parseStrict } from './strict-json.ts';

// No remote references, coercion, defaults, credentials or local file access.
const ajv = new Ajv2020({ strict: true, strictTypes: false, strictRequired: false, allErrors: true });
ajv.addSchema(schema);
const validators = new Map<string, ValidateFunction>();
export function validate(value: unknown, definition: string): void {
  let validator = validators.get(definition);
  if (!validator) {
    if (!(definition in schema.$defs)) throw new Error('Unknown rc.9 contract definition');
    validator = ajv.compile({ $ref: schema.$id + '#/$defs/' + definition });
    validators.set(definition, validator);
  }
  if (!validator(value)) throw new Error('Invalid rc.9 contract: ' + definition);
}

export function parseResponse(source: string): unknown {
  const response = parseStrict(source, 8 * 1024 * 1024);
  validate(response, 'Response');
  return response;
}

export function validateRequest(request: unknown): void {
  validate(request, 'Request');
  if (new TextEncoder().encode(canonicalJSON(request)).length > 256 * 1024) throw new Error('Request frame too large');
}
