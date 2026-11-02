import importlib.util, select, subprocess, sys, unittest, tempfile, json, time
from unittest.mock import patch
from contextlib import closing
from pathlib import Path

spec=importlib.util.spec_from_file_location('witness',Path(__file__).with_name('native-process-witness.py'))
w=importlib.util.module_from_spec(spec);spec.loader.exec_module(w)

@unittest.skipUnless(sys.platform=='darwin','Darwin kqueue witness only')
class WitnessTests(unittest.TestCase):
    def test_owned_child_exit_status_and_foreign_identity_refusal(self):
        child=subprocess.Popen(['sleep','60'])
        try:
            identity=w.processes()[child.pid]
            with closing(select.kqueue()) as q:
                admitted,refused=w.register(q,{str(child.pid):identity})
                self.assertEqual(refused,{})
                self.assertIn(str(child.pid),admitted)
                child.terminate() # Only this test's own child, never an observed user process.
                events=q.control(None,1,5)
                self.assertEqual(len(events),1)
                result=w.exit_event(events[0])
                self.assertEqual(result['state'],'exit_observed')
                self.assertEqual(result.get('signal'),15)
                self.assertEqual(result['initiator'],'unknown')
            with closing(select.kqueue()) as q:
                fake={**identity,'start':'not the original process'}
                admitted,refused=w.register(q,{str(child.pid):fake})
                self.assertEqual(admitted,{})
                self.assertTrue(refused)
        finally:
            if child.poll() is None:child.kill()
            child.wait()

class WitnessBoundsTests(unittest.TestCase):
    def test_registration_deadline_and_ps_timeout_are_fail_closed(self):
        with self.assertRaises(TimeoutError):w.remaining(time.monotonic()-1)
        with patch.object(w.subprocess,'check_output',side_effect=subprocess.TimeoutExpired('own-ps',.01)) as observed:
            with self.assertRaises(subprocess.TimeoutExpired):w.processes(time.monotonic()+1)
            self.assertLessEqual(observed.call_args.kwargs['timeout'],1)
    def test_oversize_plan_refuses_before_registration(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'plan.json';p.write_bytes(b'x'*(1024*1024+1))
            with patch.object(w,'register') as registered:
                with self.assertRaises(RuntimeError):w.watch(p,Path(d)/'out',Path(d)/'ready',1,Path(d)/'stop')
                registered.assert_not_called()

if __name__=='__main__':unittest.main()
