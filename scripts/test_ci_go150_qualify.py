import unittest, tempfile, os, plistlib
from pathlib import Path
from unittest.mock import patch
import ci_go150_qualify as m
class FakeRAM:
 instance=None
 def __init__(self):self.cleaned=False;self.key_path=Path('/Volumes/proved-RAM/private-key/key.p8');self.written=None;FakeRAM.instance=self
 def __enter__(self):return self
 def write(self,data):self.written=bytes(data)
 def verify(self):pass
 def __exit__(self,*args):self.cleaned=True
class Guards(unittest.TestCase):
 def test_all_children_exclude_credential_and_unrelated_environment(self):
  from subprocess import CompletedProcess
  with patch.dict(os.environ,self.env(),clear=True),patch.object(m.sp,'run',return_value=CompletedProcess([],0,b'{}',b''))as child:
   m.run(['codesign','--verify','fixture'])
   m.notary_read('info',Path('/Volumes/proved-RAM/key.p8'),'KEY1234567','00000000-0000-0000-0000-000000000000')
   self.assertEqual(child.call_count,2)
   for call in child.call_args_list:self.assertEqual(call.kwargs['env'],{'PATH':'/usr/bin','HOME':'/fixture'})
 def env(self):
  import base64
  return {'PATH':'/usr/bin','HOME':'/fixture','APPSTORE_CONNECT_API_KEY_ID':'KEY1234567','APPSTORE_CONNECT_API_ISSUER_ID':'00000000-0000-0000-0000-000000000000','APPSTORE_CONNECT_API_KEY_P8_BASE64':base64.b64encode(b'-----BEGIN PRIVATE KEY-----\nfixture-only\n').decode(),'UNRELATED_TOKEN':'never-in-child'}
 def test_submit_is_impossible(self):
  with patch.object(m.sp,'run')as run:
   with self.assertRaises(RuntimeError):m.notary_read('submit',Path('/no-key'),'id','issuer')
   run.assert_not_called()
 def test_wrong_archive_never_reaches_extraction(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'bad.zip';p.write_bytes(b'bad')
   with patch.object(m,'run')as run:
    with self.assertRaises(RuntimeError):m.verify_input(p,Path(d)/'work')
    run.assert_not_called()
 def test_missing_ci_credential_does_not_allocate_or_fallback(self):
  with patch.dict(os.environ,{'PATH':'/usr/bin'},clear=True),patch.object(m,'RamKeyVolume')as ram:
   with self.assertRaises(RuntimeError):m.read_with_ci_credential()
   ram.assert_not_called()
 def test_exact_original_accepted_sha_and_RAM_cleanup_required(self):
  def read(action,*args):
   self.assertNotIn('APPSTORE_CONNECT_API_KEY_P8_BASE64',os.environ);self.assertNotIn('UNRELATED_TOKEN',os.environ)
   if action=='info':return {'id':m.ORIGINAL,'status':'Accepted'},0
   return {'status':'Accepted','sha256':m.INPUT_SHA},0
  with patch.dict(os.environ,self.env(),clear=True),patch.object(m,'RamKeyVolume',FakeRAM),patch.object(m,'notary_read',side_effect=read):
   result=m.read_with_ci_credential();self.assertTrue(result['qualified']);self.assertTrue(FakeRAM.instance.cleaned)
 def test_different_apple_archive_never_qualifies(self):
  def read(action,*args):return ({'id':m.ORIGINAL,'status':'Accepted'}if action=='info'else{'status':'Accepted','sha256':'different'}),0
  with patch.dict(os.environ,self.env(),clear=True),patch.object(m,'RamKeyVolume',FakeRAM),patch.object(m,'notary_read',side_effect=read):
   with self.assertRaises(RuntimeError):m.read_with_ci_credential()
   self.assertTrue(FakeRAM.instance.cleaned)
 def test_team_visibility_failure_does_not_resubmit(self):
  with patch.dict(os.environ,self.env(),clear=True),patch.object(m,'RamKeyVolume',FakeRAM),patch.object(m,'notary_read',return_value=(None,69))as read:
   result=m.read_with_ci_credential();self.assertFalse(result['qualified']);read.assert_called_once();self.assertEqual(read.call_args.args[0],'info');self.assertTrue(FakeRAM.instance.cleaned)
 def test_changed_original_id_is_refused(self):
  with patch.dict(os.environ,self.env(),clear=True),patch.object(m,'RamKeyVolume',FakeRAM),patch.object(m,'notary_read',return_value=({'id':'different','status':'Accepted'},0)):
   with self.assertRaises(RuntimeError):m.read_with_ci_credential()
   self.assertTrue(FakeRAM.instance.cleaned)
if __name__=='__main__':unittest.main()
