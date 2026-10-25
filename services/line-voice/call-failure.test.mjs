import test from 'node:test';
import assert from 'node:assert/strict';
import {VoiceSession,callFailureCode} from './session.mjs';

test('answer timeout survives worker projection while private and unknown errors stay generic',()=>{
  assert.equal(callFailureCode(new Error('line_answer_timeout')),'line_answer_timeout');
  for(const error of [undefined,new Error('line_invite_timeout'),new Error('Authorization: private fixture'),new Error('line_answer_timeout extra')])
    assert.equal(callFailureCode(error),'line_call_failed');
});

test('unanswered invitation is not media-ready and disposal never invites again',async()=>{
  const events=[],saved=new Map();let invited=0,closed=0;
  const transport={connect:async()=>{},inviteDetailed:async()=>{invited++;},waitForAnswerDetailed:async()=>{throw new Error('PLANET reply timeout');},close:async()=>{closed++;}};
  const call=new VoiceSession({credentials:async()=>({base:{profile:{mid:'fixture'}},storage:{get:async k=>saved.get(k),set:async(k,v)=>saved.set(k,v)},close:async()=>{}}),resolvePeer:async()=>({mid:'fixture-peer'}),acquireRoute:async()=>({}),transport:()=>transport,emit:e=>events.push(e)});
  try{
    await assert.rejects(call.start({callId:'line_'+'1'.repeat(64),peerName:'fixture'}),error=>callFailureCode(error)==='line_answer_timeout');
    assert.equal(events.some(e=>e.state==='connected'),false);
    assert.equal(events.find(e=>e.status==='failed').stage,'answer');
  }finally{await call.dispose();}
  assert.equal(invited,1);assert.ok(closed>0);
});
