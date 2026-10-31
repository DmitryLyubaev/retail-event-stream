import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
from scripts.run_support import run_command, await_barrier, kill_owned_consumer, assert_unacknowledged, new_output

class HarnessTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temp.name)
        self.addCleanup(self.temp.cleanup)

    def marker(self):
        return {"phase":"fault-barrier","stage":"after-commit","runId":"r000000000001","topic":"p22.events.r000000000001.after","partition":0,"offset":0,"pid":1,"container":"a"*12,"sourceRevision":"test-source","jarSha256":"b"*64,"eventId":"00000000-0000-0000-0000-000000000001","productId":"P22_A_5"}

    def test_wrong_barrier_fails(self):
        path=self.root/"consumer.log";value=self.marker();value["stage"]="before-commit";path.write_text(json.dumps(value)+"\n")
        with self.assertRaises(ValueError):await_barrier(path,{"stage":"after-commit"},0.02)
        self.assertIn("before-commit",path.read_text())

    def test_missing_barrier_times_out(self):
        with self.assertRaises(TimeoutError):await_barrier(self.root/"missing.log",{"stage":"after-commit"},0.02)

    def test_matching_barrier_returns_observed_identity(self):
        path=self.root/"consumer.log";value=self.marker();path.write_text("startup\n"+json.dumps(value)+"\n")
        self.assertEqual(value,await_barrier(path,{"stage":"after-commit","topic":value["topic"]},0.02))

    def fake_docker(self, wrong=False, logs_fail=False):
        labels={"portfolio.project":"p22","portfolio.run":"r000000000001","com.docker.compose.project":"p22_r000000000001","com.docker.compose.service":"app"}
        actual=dict(labels)
        if wrong:actual["portfolio.run"]="rffffffffffff"
        state={"Id":"a"*64,"Name":"/owned-consumer","Config":{"Labels":actual,"Cmd":["consume"],"Hostname":"a"*12,"Image":"sha256:"+"b"*64},"Image":"sha256:"+"b"*64,"State":{"Running":True,"ExitCode":0,"OOMKilled":False}}
        state_path=self.root/"state.json";state_path.write_text(json.dumps(state));marker=self.root/"killed"
        exe=self.root/"docker"
        exe.write_text(f'''#!/usr/bin/env python3
import json,pathlib,sys
path=pathlib.Path({str(state_path)!r});state=json.loads(path.read_text())
if sys.argv[1]=="inspect": print(json.dumps([state]))
elif sys.argv[1]=="ps":
 if not state.get("removed",False): print(state["Id"])
elif sys.argv[1]=="logs": sys.exit(1 if {logs_fail!r} else 0)
elif sys.argv[1]=="stop":
 state["State"]["Running"]=False;path.write_text(json.dumps(state))
elif sys.argv[1]=="rm":
 state["removed"]=True;path.write_text(json.dumps(state))
elif sys.argv[1] in ("network","volume"): pass
elif sys.argv[1]=="kill":
 state["State"].update(Running=False,ExitCode=137)
 path.write_text(json.dumps(state));pathlib.Path({str(marker)!r}).write_text("KILL")
 print(state["Id"])
else: sys.exit(2)
''')
        exe.chmod(0o755)
        return labels,marker,state_path

    def test_wrong_container_labels_refuse_kill(self):
        labels,marker,_=self.fake_docker(wrong=True)
        with patch.dict(os.environ,{"PATH":str(self.root)+os.pathsep+os.environ["PATH"]}):
            with self.assertRaises(PermissionError):kill_owned_consumer("a"*64,labels)
        self.assertFalse(marker.exists())

    def test_owned_consumer_is_killed_and_exit_is_verified(self):
        labels,marker,state_path=self.fake_docker()
        with patch.dict(os.environ,{"PATH":str(self.root)+os.pathsep+os.environ["PATH"]}):kill_owned_consumer("a"*64,labels)
        self.assertTrue(marker.exists());self.assertFalse(json.loads(state_path.read_text())["State"]["Running"])

    def test_log_failure_still_stops_owned_resources(self):
        from scripts.cleanup import cleanup_owned
        _,_,state_path=self.fake_docker(logs_fail=True)
        with patch.dict(os.environ,{"PATH":str(self.root)+os.pathsep+os.environ["PATH"]}):
            try:report=cleanup_owned("r000000000001",self.root)
            except Exception as error:self.fail("Optional log failure prevented owned cleanup: "+str(error))
        self.assertTrue(json.loads(state_path.read_text()).get("removed"))
        self.assertFalse(report["passed"])

    def test_advanced_offset_fails_crash_claim(self):
        with self.assertRaises(AssertionError):assert_unacknowledged({"0":1,"1":None,"2":None},0,0)

    def test_command_timeout_preserves_output(self):
        with self.assertRaises(subprocess.TimeoutExpired) as caught:run_command([sys.executable,"-c","import time; print('started',flush=True); time.sleep(3)"],0.2)
        output=caught.exception.output
        if isinstance(output,bytes):output=output.decode()
        self.assertIn("started",output)

    def test_real_command_output_is_retained(self):
        result=run_command([sys.executable,"-c","print('measured')"],2)
        self.assertEqual("measured",result.stdout.strip())

    def test_unsafe_output_path_refused(self):
        with self.assertRaises(ValueError):new_output(self.root/"outside",self.root)

    def test_symlinked_artifacts_cannot_escape_workspace(self):
        workspace=self.root/"workspace";workspace.mkdir();external=self.root/"external";external.mkdir()
        (workspace/"artifacts").symlink_to(external,target_is_directory=True)
        with self.assertRaises(ValueError):new_output(workspace/"artifacts"/"new",workspace)
        self.assertFalse((external/"new").exists())

    def test_existing_evidence_is_not_overwritten(self):
        path=self.root/"artifacts"/"existing";path.mkdir(parents=True);(path/"keep").write_text("original")
        with self.assertRaises(FileExistsError):new_output(path,self.root)
        self.assertEqual("original",(path/"keep").read_text())

    def test_new_evidence_directory_is_created(self):
        path=self.root/"artifacts"/"new"
        self.assertEqual(path,new_output(path,self.root));self.assertTrue(path.is_dir())

    def oracle_sample(self):
        root=pathlib.Path(__file__).resolve().parents[1]
        records=json.loads((root/"fixtures/events.json").read_text())
        record=records[0]
        fingerprint="e2980505cae8c881c8ae6232b2195505ce00a6d25eda0d24ac640cb7104cb3d3"
        event={"productId":"P22_A_5","eventId":"00000000-0000-0000-0000-000000000001"}
        row={"topic":"p22.oracle","partition":0,"offset":0,"disposition":"APPLIED","reason":None,"postBalance":-1,"ordinal":1,"lateEvent":False,"fingerprint":fingerprint,"rawKey":record["keyBase64"],"rawValue":record["valueBase64"],**event}
        snapshot={"inventory":[{"productId":"P22_A_5","balance":-1,"negativeStock":True,"ordinal":1},{"productId":"P22_B_0","balance":10,"negativeStock":False,"ordinal":0},{"productId":"P22_C_2","balance":0,"negativeStock":False,"ordinal":0}],"receipts":[{**event,"rawKey":record["keyBase64"],"rawValue":record["valueBase64"],"delta":-3}],"processing":[row]}
        manifest={"boundary":{"topic":"p22.oracle","beginning":{"0":0,"1":0,"2":0},"end":{"0":1,"1":0,"2":0}},"records":[{"index":0,"topic":"p22.oracle","partition":0,"offset":0,"fingerprint":fingerprint}]}
        expected={"counts":{"APPLIED":1},"balances":{"P22_A_5":-1,"P22_B_0":10,"P22_C_2":0},"records":[{"index":0,"disposition":"APPLIED","postBalance":-1,"ordinal":1,"lateEvent":False}]}
        seed={"P22_A_5":2,"P22_B_0":10,"P22_C_2":0}
        return snapshot,manifest,[record],expected,seed,event

    def test_snapshot_oracle_checks_real_effects(self):
        from scripts.demo import verify_snapshot
        snapshot,manifest,records,expected,seed,_=self.oracle_sample()
        self.assertEqual(1,verify_snapshot(snapshot,manifest,records,expected,seed).get("acceptedReceipts"))

    def test_wrong_intermediate_balance_fails_oracle(self):
        from scripts.demo import verify_snapshot
        snapshot,manifest,records,expected,seed,_=self.oracle_sample();snapshot["processing"][0]["postBalance"]=0
        with self.assertRaises(AssertionError):verify_snapshot(snapshot,manifest,records,expected,seed)

    def test_missing_receipt_fails_oracle(self):
        from scripts.demo import verify_snapshot
        snapshot,manifest,records,expected,seed,_=self.oracle_sample();snapshot["receipts"]=[]
        with self.assertRaises(AssertionError):verify_snapshot(snapshot,manifest,records,expected,seed)

    def test_existing_effect_cannot_be_claimed_as_precommit_rollback(self):
        from scripts.demo import verify_crash_state
        snapshot,manifest,_,_,_,event=self.oracle_sample()
        with self.assertRaises(AssertionError):verify_crash_state(snapshot,{"0":None,"1":None,"2":None},manifest["records"][0],event,2,"before-commit")

    def test_postcommit_effect_and_uncommitted_offset_are_verified(self):
        from scripts.demo import verify_crash_state
        snapshot,manifest,_,_,_,event=self.oracle_sample()
        result=verify_crash_state(snapshot,{"0":None,"1":None,"2":None},manifest["records"][0],event,-1,"after-commit")
        self.assertEqual(1,result.get("targetReceipts"))

    def test_optimized_python_cannot_disable_acceptance_assertions(self):
        result=run_command([sys.executable,"-O","-c","import scripts.demo"],5)
        self.assertNotEqual(0,result.returncode)

if __name__=="__main__":unittest.main()
