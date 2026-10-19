"""Exact Go150 artifact qualification. There is deliberately no submit operation."""
from pathlib import Path
import base64, hashlib, json, os, plistlib, re, subprocess as sp, sys, time
from ci_ram_key_volume import RamKeyVolume

SOURCE='b1dd779074c9a00e2aa9fca320558741320174b0'
INPUT_SHA='085c3c7db64b535b740e45aa0e5152451b7be4ca2b827cce824523c525468d06'
BINARY_SHA='354034bee209266cae83d2e887cae9975484c047ffba0dec3e439a47770ca9eb'
PREVIOUS_UNRESOLVED=None
ORIGINAL='5500a305-22a9-4a79-a5cc-b58f698d41e6'
TEAM='UPWLTJL6S2'
VERSION='0.2.153'
ARCH='arm64'
OUTPUT_NAME='Everything-Go-0.2.153-ci-qualified-arm64.zip'
ACTIONS={'info','log'}
CHILD_ENV={'PATH','HOME','TMPDIR','USER','LOGNAME','LANG','LC_ALL','DEVELOPER_DIR'}
def child_env():return {k:v for k,v in os.environ.items()if k in CHILD_ENV}
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def run(args,timeout=45):
 r=sp.run(args,capture_output=True,timeout=timeout,env=child_env())
 if r.returncode:raise RuntimeError('QUALIFICATION_COMMAND_FAILED_'+Path(args[0]).name+'_'+str(r.returncode))
 return r.stdout+r.stderr
def save(path,value):
 fd=os.open(path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'w')as f:json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
def verify_input(archive,workspace):
 if sha(archive)!=INPUT_SHA:raise RuntimeError('INPUT_HASH_MISMATCH')
 workspace.mkdir(mode=0o700)
 run(['ditto','-x','-k',str(archive),str(workspace)])
 app=workspace/'Everything Go.app';binary=app/'Contents/MacOS/everything-go'
 if sha(binary)!=BINARY_SHA:raise RuntimeError('BINARY_HASH_MISMATCH')
 info=plistlib.loads((app/'Contents/Info.plist').read_bytes())
 if (info.get('CFBundleIdentifier'),info.get('CFBundleShortVersionString'),info.get('BridgeSourceRevision'))!=('com.everything-go.app',VERSION,SOURCE):raise RuntimeError('APP_IDENTITY_MISMATCH')
 run(['codesign','--verify','--deep','--strict',str(app)])
 signature=run(['codesign','-d','--verbose=4',str(app)]).decode()
 if 'TeamIdentifier='+TEAM not in signature or 'Authority=Developer ID Application: YuDi Huang ('+TEAM+')' not in signature or '(runtime)'not in signature or 'Timestamp='not in signature:raise RuntimeError('SIGNATURE_IDENTITY_MISMATCH')
 if run(['lipo','-archs',str(binary)]).decode().strip()!=ARCH:raise RuntimeError('ARCHITECTURE_MISMATCH')
 build=run(['go','version','-m',str(binary)]).decode()
 if 'vcs.revision='+SOURCE not in build or 'vcs.modified=false'not in build:raise RuntimeError('BUILD_SOURCE_MISMATCH')
 return app

def notary_read(action,key_path,key_id,issuer):
 if action not in ACTIONS:raise RuntimeError('NOTARY_EFFECT_FORBIDDEN')
 # The original submission is fixed; no user-controlled routing or new request.
 r=sp.run(['xcrun','notarytool',action,ORIGINAL,'--key',str(key_path),'--key-id',key_id,'--issuer',issuer,'--output-format','json'],capture_output=True,timeout=45,env=child_env())
 if r.returncode:return None,r.returncode
 return json.loads(r.stdout),0

def read_with_ci_credential():
 # GitHub binds the existing encrypted secret only to this step. It is removed
 # immediately; decoded PEM never enters argv, child env, disk temp or logs.
 encoded=os.environ.pop('APPSTORE_CONNECT_API_KEY_P8_BASE64','')
 key_id=os.environ.pop('APPSTORE_CONNECT_API_KEY_ID','')
 issuer=os.environ.pop('APPSTORE_CONNECT_API_ISSUER_ID','')
 allowed=child_env()
 os.environ.clear();os.environ.update(allowed)
 if not encoded or not re.fullmatch(r'[A-Za-z0-9]{6,20}',key_id)or not re.fullmatch(r'[A-Fa-f0-9-]{36}',issuer):raise RuntimeError('EXISTING_CI_CREDENTIAL_UNAVAILABLE')
 key=bytearray(base64.b64decode(''.join(encoded.split()),validate=True));del encoded
 try:
  if not key.startswith(b'-----BEGIN PRIVATE KEY-----'):raise RuntimeError('CI_KEY_FORMAT_REFUSED')
  with RamKeyVolume()as ram:
   ram.write(key)
   for n in range(len(key)):key[n]=0
   ram.verify()
   deadline=time.monotonic()+480
   while True:
    data,code=notary_read('info',ram.key_path,key_id,issuer)
    if code:return {'state':'HOLD_ORIGINAL_SUBMISSION_NOT_READABLE','query_exit':code,'qualified':False}
    if data.get('id')!=ORIGINAL:raise RuntimeError('ORIGINAL_RECEIPT_ID_MISMATCH')
    status=data.get('status')
    if status=='Accepted':break
    if status!='In Progress' or time.monotonic()>=deadline:return {'state':status or 'UNKNOWN','qualified':False}
    time.sleep(30);ram.verify()
   proof,code=notary_read('log',ram.key_path,key_id,issuer)
   if code or proof.get('sha256')!=INPUT_SHA or proof.get('status')!='Accepted':raise RuntimeError('APPLE_LOG_HASH_MISMATCH')
  return {'state':'APPLE_ACCEPTED_INPUT_VERIFIED','qualified':True,'RAM_cleanup_verified':ram.cleaned}
 finally:
  for n in range(len(key)):key[n]=0

def main():
 mode= sys.argv[1];archive=Path(sys.argv[2]).resolve();root=Path(sys.argv[3]).resolve();root.mkdir(mode=0o700,parents=True,exist_ok=True)
 if mode=='verify':
  verify_input(archive,root/'verified-input');save(root/'input-verified.json',{'state':'EXACT_SIGNED_INPUT_VERIFIED','input_sha256':INPUT_SHA,'binary_sha256':BINARY_SHA,'source':SOURCE,'team':TEAM});return
 if mode!='qualify':raise RuntimeError('UNKNOWN_MODE')
 # Repeat the non-secret identity gate before even reading CI credential inputs.
 app=verify_input(archive,root/'qualification-input')
 receipt={'submission_id':ORIGINAL,'previous_unresolved_submission_id':PREVIOUS_UNRESOLVED,'user_authorized_distinct_submission':True,'ci_submitted':False,'input_sha256':INPUT_SHA,'source':SOURCE,'binary_sha256':BINARY_SHA,'runtime_changed':False}
 try:result=read_with_ci_credential()
 except Exception as exc:
  save(root/'go150-ci-receipt.json',{**receipt,'state':'HOLD','error_class':type(exc).__name__,'qualified':False});return 4
 receipt.update(result)
 if not result['qualified']:save(root/'go150-ci-receipt.json',receipt);return 4
 receipt['post_checks']=[]
 for name,args in [('staple',['xcrun','stapler','staple',str(app)]),('ticket_validate',['xcrun','stapler','validate',str(app)]),('signature',['codesign','--verify','--deep','--strict',str(app)]),('gatekeeper',['spctl','--assess','--type','execute',str(app)])]:
  try:run(args)
  except RuntimeError as exc:
   save(root/'go150-ci-receipt.json',{**receipt,'qualified':False,'state':'POST_QUALIFICATION_HOLD','failed_check':name,'safe_error_code':str(exc)});raise
  receipt['post_checks'].append(name)
 if sha(app/'Contents/MacOS/everything-go')!=BINARY_SHA:raise RuntimeError('POST_QUALIFICATION_BINARY_CHANGED')
 output=root/OUTPUT_NAME;run(['ditto','-c','-k','--sequesterRsrc','--keepParent',str(app),str(output)])
 receipt.update(state='CI_SIGNED_NOTARIZED_STAPLED_QUALIFIED',archive_sha256=sha(output),archive_name=output.name,qualified=True)
 save(root/'go150-ci-receipt.json',receipt)
 return 0
if __name__=='__main__':sys.exit(main()or 0)
