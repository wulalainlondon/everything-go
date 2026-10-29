import {apiVersion,schemaHash,mutationOperations,resultDefinitions} from './schema-metadata.ts';
import {validate,validateRequest,parseResponse} from './runtime.ts';
import {canonicalJSON,inputHash} from './hash.ts';
import type {Request,Response,Receipt,Operation,InputOf,ResultOf,Identity,Capabilities} from './types.ts';
export type MutationOperation=typeof mutationOperations[number];
export const isMutation=(op:string):op is MutationOperation=>(mutationOperations as readonly string[]).includes(op);
export interface PendingIntent {namespace:string;namespaceGeneration:number;authority:string;request:Request&{operation:MutationOperation;idempotency_key:string};inputHash:string;state:'prepared'|'unknown'|'resolved';receipt?:Receipt}
export interface IntentJournal {find(op:MutationOperation,key:string):Promise<PendingIntent|undefined>;save(value:PendingIntent):Promise<void>;claim(value:PendingIntent):Promise<boolean>}
export interface Lease {authority:string;namespace:string;generation:number;key:string;manager:Identity}
export interface Port {lease():Lease|null;send(raw:string):Promise<string>;journal:IntentJournal;correlation():string}
export class ControllerResultsClient {
 private port:Port;private retired=false;private capabilities:Capabilities|null=null;private capabilityLease:Lease|null=null;
 constructor(port:Port){this.port=port}
 retire(){this.retired=true}
 private capture():Lease{const v=this.port.lease();if(this.retired||!v||!v.authority||!v.namespace||!v.key||!Number.isSafeInteger(v.generation))throw Error('scope_changed');validate(v.manager,"Identity");if(v.manager.authority_instance_id!==v.authority||!v.manager.session_id||!v.manager.thread_id)throw Error("scope_changed");return structuredClone(v)}
 private check(v:Lease){if(this.retired||canonicalJSON(this.port.lease())!==canonicalJSON(v))throw Error('scope_changed')}
 private async invoke<O extends Operation>(request:Request,lease:Lease):Promise<ResultOf<O>>{
  validateRequest(request);if("manager" in request.input&&canonicalJSON(request.input.manager)!==canonicalJSON(lease.manager))throw Error("scope_changed");this.check(lease);const raw=await this.port.send(canonicalJSON(request));this.check(lease);const response=parseResponse(raw) as Response;
  if(response.authority_instance_id!==lease.authority||response.correlation_id!==request.correlation_id||response.operation!==request.operation)throw Error('correlation_or_authority_mismatch');
  if(!response.ok)throw Object.assign(Error(response.error.code),{response});
  validate(response.result,resultDefinitions[request.operation]);if(request.operation==='capabilities'&&(response.result.kind!=='capabilities'||response.result.schema_hash!==schemaHash))throw Error('schema_mismatch');
  if(request.operation==="capabilities"){this.capabilities=response.result as Capabilities;this.capabilityLease=structuredClone(lease);}this.check(lease);return response.result as ResultOf<O>;
 }
 async read<O extends Exclude<Operation,MutationOperation>>(op:O,input:InputOf<O>):Promise<ResultOf<O>>{if(isMutation(op))throw Error('mutation_requires_original_intent');const lease=this.capture();return this.invoke<O>({type:'task_api_controller_results_request',api_version:apiVersion,correlation_id:this.port.correlation(),operation:op,input} as Request,lease)}
 async prepare<O extends MutationOperation>(op:O,input:InputOf<O>,key:string):Promise<PendingIntent>{if(!isMutation(op)||!key)throw Error('explicit_stable_key_required');const lease=this.capture();if(!this.capabilities||canonicalJSON(this.capabilityLease)!==canonicalJSON(lease)||!this.capabilities.allowed_operations.includes(op))throw Error("capability_unverified");const request={type:'task_api_controller_results_request',api_version:apiVersion,correlation_id:this.port.correlation(),operation:op,idempotency_key:key,input} as PendingIntent['request'];validateRequest(request);const hash=await inputHash(op,input);this.check(lease);const old=await this.port.journal.find(op,key);this.check(lease);if(old){if(old.inputHash!==hash||old.namespace!==lease.namespace||old.namespaceGeneration!==lease.generation||old.authority!==lease.authority)throw Error('original_intent_conflict');return old}
  const intent:PendingIntent={namespace:lease.namespace,namespaceGeneration:lease.generation,authority:lease.authority,request,inputHash:hash,state:'prepared'};await this.port.journal.save(intent);this.check(lease);return structuredClone(intent);
 }
 private verifyReceipt(intent:PendingIntent,result:Receipt){validate(result,'Receipt');if(result.intent_id!==intent.request.idempotency_key||result.input_hash!==intent.inputHash||result.operation!==intent.request.operation||result.state!=='applied')throw Error('original_receipt_mismatch');
 const input=intent.request.input;
 if(intent.request.operation==='set_dispatch_return_control'){
  const q=input as InputOf<'set_dispatch_return_control'>;
  if(!result.control||result.control.revision!==q.expected_revision+1||result.control.mode!==q.mode||result.control.paused!==q.paused||result.control.source_intent!==intent.request.idempotency_key)throw Error('original_control_mismatch');
 }else{
  const q=input as InputOf<'ack_dispatch_results'>;
  if(canonicalJSON(q.versions)!==canonicalJSON(result.versions))throw Error('original_versions_mismatch');
  if(intent.request.operation==='ack_dispatch_results'){
   const p=result.native_provenance;
   if(!p||canonicalJSON(p.source)!==canonicalJSON(q.manager)||p.read_receipt_id!==q.read_receipt_id||!p.request_id||!p.native_turn_id||!p.tool_call_id)throw Error('native_provenance_unverified');
  }else if(result.native_provenance)throw Error('paired_provenance_conflict');
 }
}
 private own(intent:PendingIntent,lease:Lease){if(intent.namespace!==lease.namespace||intent.namespaceGeneration!==lease.generation||intent.authority!==lease.authority)throw Error('scope_changed')}
 async send(intent:PendingIntent):Promise<Receipt>{intent=structuredClone(intent);const lease=this.capture();this.own(intent,lease);if(intent.state!=='prepared')throw Error('unknown_only_original_lookup');const claimed=await this.port.journal.claim(intent);this.check(lease);if(!claimed)throw Error('already_claimed_no_replay');intent.state='unknown';const result=await this.invoke(intent.request,lease) as Receipt;this.verifyReceipt(intent,result);this.check(lease);await this.port.journal.save({...intent,state:'resolved',receipt:result});this.check(lease);return result}
 async recover(op:MutationOperation,key:string):Promise<Receipt>{const lease=this.capture();const intent=await this.port.journal.find(op,key);this.check(lease);if(!intent)throw Error('original_intent_missing');this.own(intent,lease);if(intent.state==='resolved'&&intent.receipt)return intent.receipt;const input=intent.request.input as {manager:unknown};const result=await this.read('operation_get',{manager:input.manager,original_intent_id:key,expected_input_hash:intent.inputHash} as InputOf<'operation_get'>);this.check(lease);this.verifyReceipt(intent,result);await this.port.journal.save({...intent,state:'resolved',receipt:result});this.check(lease);return result}
}
