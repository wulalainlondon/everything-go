import {test} from 'node:test';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {dirname,join} from 'node:path';
import {createHash} from 'node:crypto';
import {ControllerResultsClient} from './client.ts';
import {StorageIntentJournal} from './journal.ts';
import {validateRequest,parseResponse,validate} from './runtime.ts';
import {schemaHash} from './schema-metadata.ts';
import {canonicalJSON,inputHash} from './hash.ts';
const path=process.env.CONTROLLER_RESULTS_WIRE_MANIFEST;
test('actual Go HTTP bytes, original client mutation/lookup and cross-language hash',{skip:!path},async()=>{
 const root=dirname(path!);const m=JSON.parse(readFileSync(path!,'utf8'));assert.equal(m.schema_hash,schemaHash);
 const rows=new Map<string,{request:any,response:string}>();for(const e of m.entries){const q=readFileSync(join(root,e.request_path));const r=readFileSync(join(root,e.response_path));for(const [bytes,size,sha] of [[q,e.request_bytes,e.request_sha256],[r,e.response_bytes,e.response_sha256]]as const){assert.equal(bytes.length,size);assert.equal(createHash('sha256').update(bytes).digest('hex'),sha)}const request=JSON.parse(q.toString());validateRequest(request);const response:any=parseResponse(r.toString());assert.equal(response.correlation_id,request.correlation_id);assert.equal(response.ok,e.ok);rows.set(e.name,{request,response:r.toString()})}
 const order=['capabilities','inbox','members','full-original-final','human-seen','seen-original-lookup','events','status','control-pause','control-original-lookup','wrong-parent-generation','grant-revoked'];let position=0;const manager=rows.get('inbox')!.request.input.manager;
 const memory=new Map<string,string>();const storage={get length(){return memory.size},key:(i:number)=>[...memory.keys()][i]??null,getItem:(k:string)=>memory.get(k)??null,setItem:(k:string,v:string)=>void memory.set(k,v)};const journal=new StorageIntentJournal(storage,{request:async(_key,fn)=>fn()},'actual-wire-synthetic');
 const client=new ControllerResultsClient({lease:()=>({authority:manager.authority_instance_id,namespace:'actual-wire-synthetic',generation:1,key:'synthetic-original-paired-epoch',manager}),journal,correlation:()=>rows.get(order[position])!.request.correlation_id,send:async raw=>{const pair=rows.get(order[position++])!;assert.equal(canonicalJSON(JSON.parse(raw)),canonicalJSON(pair.request));return pair.response}});
 for(const name of order){const pair=rows.get(name)!;const q=pair.request;if(q.operation==='mark_dispatch_results_seen'||q.operation==='set_dispatch_return_control'){const p=await client.prepare(q.operation,q.input,q.idempotency_key);await client.send(p)}else if(name==='wrong-parent-generation'){// Exact source is rejected locally before any transport; raw server denial was independently parsed above.
 await assert.rejects(client.read(q.operation,q.input),/scope_changed/);position++;
 }else if(name==='grant-revoked'){await assert.rejects(client.read(q.operation,q.input),/permission/)}else await client.read(q.operation,q.input)}
 assert.equal(position,order.length);
 const golden=JSON.parse(readFileSync(join(root,'HASH_GOLDEN_RC9_R1.json'),'utf8'));assert.equal(golden.schema_hash,schemaHash);for(const v of golden.vectors)assert.equal(await inputHash(v.operation,v.input),v.expected_hash);
 const native=JSON.parse(readFileSync(join(root,'native/native-ack.response.json'),'utf8'));validate(native,'Response');assert.ok(native.result.native_provenance);const lookup=JSON.parse(readFileSync(join(root,'native/parent-original-lookup.response.json'),'utf8'));assert.deepEqual(lookup.result,native.result);
});
