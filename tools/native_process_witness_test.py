import importlib.util, select, subprocess, sys, unittest
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

if __name__=='__main__':unittest.main()
