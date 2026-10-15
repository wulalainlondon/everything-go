// Narrow reply-correlation port for the pinned SDK. See TRANSPORT_PATCH.txt.
import {createHash} from 'node:crypto';
import {readFile,mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {pathToFileURL} from 'node:url';

export const baselineTransportSHA256='58bdce8c06193c99559001469d19bbe50cc9dd7ccaddf2789d3f908d0ac57a79';
export const sdkCallURL=new URL(import.meta.resolve('@evex/linejs/call'));
const sourceURL=new URL('./planet/transport.js',sdkCallURL);
const hash=value=>createHash('sha256').update(value).digest('hex');
function replaceOnce(source,before,after){
  const index=source.indexOf(before);
  if(index<0||source.indexOf(before,index+before.length)>=0)throw new Error('line_transport_patch_shape_invalid');
  return source.slice(0,index)+after+source.slice(index+before.length);
}
export function patchTransportSource(source){
  if(hash(source)!==baselineTransportSHA256)throw new Error('line_transport_patch_baseline_mismatch');
  const helper=`  #planetReplyHdr(msgId, request) {
    if (!this.#sessId) throw new Error("connect first");
    if (request?.tranId?.length !== 16 || request.tranSeq === undefined) {
      throw new Error("line_reply_transaction_required");
    }
    return {
      userId: this.#opts.localMid,
      msgId,
      sessId: request.sessId?.length ? request.sessId : this.#sessId,
      tranId: request.tranId,
      tranSeq: request.tranSeq,
      locNonce: this.#locNonce,
      rmtNonce: this.#rmtNonce
    };
  }
`;
  source=replaceOnce(source,'  async #sendEnvelope(body, opts = {}) {',helper+'  async #sendEnvelope(body, opts = {}) {');
  source=replaceOnce(source,'    const hdr = this.#planetHdr(opts.msgId);','    const hdr = Object.hasOwn(opts, "replyTo") ? this.#planetReplyHdr(opts.msgId ?? this.#msgIdCounter++, opts.replyTo) : this.#planetHdr(opts.msgId);');
  for(const id of ['ccMsgId(CC_MSG.CONN_RSP)','ccMsgId(CC_MSG.INFO_RSP)','CASSINI_MSG_ID_MC_DATA_RSP','CASSINI_MSG_ID_MC_JOIN_RSP','CASSINI_MSG_ID_MC_CHANGE_RSP']){
    source=replaceOnce(source,`      msgId: ${id}\n`,`      msgId: ${id},\n      replyTo: request.message.hdr\n`);
  }
  return source;
}
let loading;
export function loadPatchedTransport(){return loading??=load();}
async function load(){
  const original=await readFile(sourceURL,'utf8'),patched=patchTransportSource(original);
  // Resolve only static relative imports against the real package. SDK files
  // stay untouched; temporary code contains no credentials and is removed after load.
  const runnable=patched.replace(/from "(\.{1,2}\/[^"\n]+)"/g,(_,specifier)=>`from ${JSON.stringify(new URL(specifier,sourceURL).href)}`);
  const directory=await mkdtemp(join(tmpdir(),'linejs-reply-'));
  try{
    const file=join(directory,'transport.mjs');await writeFile(file,runnable,{mode:0o600,flag:'wx'});
    const module=await import(pathToFileURL(file).href);
    return {...module,patchInfo:{sdkVersion:'3.4.2',variant:'reply-correlation-v1',baselineSHA256:baselineTransportSHA256,patchedSHA256:hash(patched)}};
  }finally{await rm(directory,{recursive:true,force:true});}
}
