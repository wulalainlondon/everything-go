import { readFileSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';

// Build-time generation only. The portable runtime never opens local files or
// acquires credentials. Schema bytes are copied from the reviewed Go contract.
const bytes = readFileSync(new URL('./api-contract.schema.json', import.meta.url));
const contract = JSON.parse(bytes.toString('utf8'));
function type(value) {
  if (value.$ref) return value.$ref.split('/').at(-1);
  if ('const' in value) return JSON.stringify(value.const);
  if (value.enum) return value.enum.map(x => JSON.stringify(x)).join(' | ');
  // Object conditionals refine the base shape at runtime (for example a
  // nonempty prompt OR media). They must not erase its identity/route fields.
  if (value.type !== 'object' && (value.oneOf || value.anyOf)) return '(' + (value.oneOf ?? value.anyOf).map(type).join(' | ') + ')';
  if (Array.isArray(value.type)) return value.type.map(t => type({ ...value, type: t })).join(' | ');
  if (value.type === 'object') {
    const required = new Set(value.required ?? []);
    const fields = Object.entries(value.properties ?? {}).map(([name, v]) =>
      JSON.stringify(name) + (required.has(name) ? '' : '?') + ': ' + type(v));
    const object = '{ ' + fields.join('; ') + ' }';
    return value.additionalProperties === false ? object : '(' + object + ' & Record<string, unknown>)';
  }
  if (value.type === 'array') return 'Array<' + type(value.items ?? {}) + '>';
  return { string: 'string', integer: 'number', number: 'number', boolean: 'boolean', null: 'null' }[value.type] ?? 'unknown';
}
const hash = createHash('sha256').update(bytes).digest('hex');
const resultDefinitions = {};
for (const request of contract.$defs.Request.oneOf) {
  const operation = request.properties.operation.const;
  const definition = ({capabilities:'Capabilities',read_dispatch_inbox:'Page',read_dispatch_group:'MembersPage',read_dispatch_final:'Bytes',get_dispatch_return_status:'Status',dispatch_result_events:'Events',ack_dispatch_results:'Receipt',mark_dispatch_results_seen:'Receipt',set_dispatch_return_control:'Receipt',operation_get:'Receipt'})[operation];
  if (!definition || [definition].flat().some(name => !(name in contract.$defs))) throw Error('Unmapped operation result: '+operation);
  resultDefinitions[operation] = definition;
}
const outputs = {
  'types.ts': '// Generated from exact rc.9 schema. Runtime validation remains authoritative.\n' +
    Object.entries(contract.$defs).map(([name, definition]) => 'export type ' + name + ' = ' + type(definition) + ';\n').join('') +
    '\nexport type Operation = Request["operation"];\nexport type InputOf<O extends Operation> = Extract<Request, {operation: O}>["input"];\n' +
    'export interface Results {\n' + Object.entries(resultDefinitions).map(([op,defs]) => JSON.stringify(op)+': '+[defs].flat().join(' | ')+';\n').join('') + '}\nexport type ResultOf<O extends Operation> = Results[O];\n',
  'schema-metadata.ts': '// Generated from exact schema bytes.\nexport const schemaHash = ' + JSON.stringify(hash) + ' as const;\nexport const apiVersion = "1.0.0-rc.9" as const;\n',
};
outputs['schema-metadata.ts'] += 'export const mutationOperations = ' + JSON.stringify(contract.$defs.Request.oneOf.filter(v => v.required.includes('idempotency_key')).map(v => v.properties.operation.const)) + ' as const;\n';
outputs['schema-metadata.ts'] += 'export const resultDefinitions = ' + JSON.stringify(resultDefinitions) + ' as const;\n';
for (const [name, output] of Object.entries(outputs)) {
  const url = new URL('./' + name, import.meta.url);
  if (process.argv.includes('--check')) {
    if (readFileSync(url, 'utf8') !== output) throw new Error('Generated file is stale: ' + name);
  } else writeFileSync(url, output);
}
