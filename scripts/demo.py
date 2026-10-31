"""Source-bound, actual-process acceptance. All business records are synthetic."""
if not __debug__:
    raise RuntimeError("Acceptance verification requires Python without optimization")
import argparse
import base64
from collections import Counter
import hashlib
import json
import os
import pathlib
import re
import subprocess
import time
import uuid

if __package__:
    from .run_support import ROOT,run_command,checked,await_barrier,kill_owned_consumer,assert_unacknowledged,new_output,raw_fingerprint,fixture_identity,json_object_from_output,owner_labels,inspect_container
    from .cleanup import cleanup_owned
else:
    from run_support import ROOT,run_command,checked,await_barrier,kill_owned_consumer,assert_unacknowledged,new_output,raw_fingerprint,fixture_identity,json_object_from_output,owner_labels,inspect_container
    from cleanup import cleanup_owned

def snapshot_hash(snapshot):
    return hashlib.sha256(json.dumps(snapshot,sort_keys=True,separators=(",",":")).encode()).hexdigest()

def verify_snapshot(snapshot,manifest,records,expected,seed):
    inventory={r["productId"]:r for r in snapshot["inventory"]}
    assert len(inventory)==len(snapshot["inventory"]) and set(inventory)==set(expected["balances"]),"Unexpected inventory products"
    actual={(r["topic"],r["partition"],r["offset"]):r for r in snapshot["processing"]}
    acknowledgements={(r["topic"],r["partition"],r["offset"]):r for r in manifest["records"]}
    assert len(actual)==len(snapshot["processing"]) and len(acknowledgements)==len(manifest["records"]) and set(actual)==set(acknowledgements),"Log-coordinate coverage differs"
    oracle={r["index"]:r for r in expected["records"]};inputs={r["index"]:r for r in records}
    assert set(oracle)==set(inputs)=={r["index"] for r in manifest["records"]} and len(manifest["records"])==len(oracle),"Fixture index coverage differs"
    counts=Counter(r["disposition"] for r in actual.values());assert counts==Counter(expected["counts"]),"Disposition counts differ"
    assert len(snapshot["receipts"])==counts["APPLIED"],"Accepted receipt count differs"
    prior=dict(seed);ordinals=Counter();expected_receipts={}
    by_index={r["index"]:r for r in manifest["records"]}
    for index,want in sorted(oracle.items()):
        ack=by_index[index];assert ack["topic"]==manifest["boundary"]["topic"],"Acknowledgement source differs"
        row=actual[(ack["topic"],ack["partition"],ack["offset"])];record=inputs[index]
        for field in ["disposition","postBalance","ordinal","lateEvent"]:
            assert type(row[field]) is type(want[field]) and row[field]==want[field],"Oracle differs at index "+str(index)+" field "+field
        assert row["rawKey"]==record["keyBase64"] and row["rawValue"]==record["valueBase64"],"Raw bytes differ"
        assert row["fingerprint"]==ack["fingerprint"]==raw_fingerprint(record),"Fingerprint differs"
        if want["disposition"]=="REJECTED":assert isinstance(row["reason"],str) and row["reason"],"Missing rejection reason"
        else:assert row["reason"] is None,"Unexpected success reason"
        if want["disposition"]=="APPLIED":
            event=json.loads(base64.b64decode(record["valueBase64"]));identity=(event["productId"],event["eventId"])
            assert identity not in expected_receipts,"Oracle repeats an applied identity"
            delta=want["postBalance"]-prior[event["productId"]];prior[event["productId"]]=want["postBalance"];ordinals[event["productId"]]+=1
            expected_receipts[identity]=(record,delta)
    assert len(expected_receipts)==len(snapshot["receipts"]),"Unexpected accepted identities"
    observed=set()
    for receipt in snapshot["receipts"]:
        identity=(receipt["productId"],receipt["eventId"]);assert identity in expected_receipts and identity not in observed,"Unexpected or duplicate receipt"
        observed.add(identity);record,delta=expected_receipts[identity]
        assert receipt["rawKey"]==record["keyBase64"] and receipt["rawValue"]==record["valueBase64"] and type(receipt["delta"]) is int and receipt["delta"]==delta,"Receipt effect/bytes differ"
    for product,balance in expected["balances"].items():
        row=inventory[product];assert type(row["balance"]) is int and row["balance"]==balance,"Balance differs"
        assert type(row["negativeStock"]) is bool and row["negativeStock"]==(balance<0),"Negative-stock flag differs"
        assert type(row["ordinal"]) is int and row["ordinal"]==ordinals[product],"Applied ordinal differs"
    return {"balances":{k:v["balance"] for k,v in inventory.items()},"dispositions":dict(counts),"acceptedReceipts":len(snapshot["receipts"]),"processingRecords":len(actual),"lateEvents":sum(r["lateEvent"] for r in actual.values()),"snapshotSha256":snapshot_hash(snapshot)}

def verify_crash_state(snapshot,offsets,ack,event,expected_balance,stage):
    assert stage in {"before-commit","after-commit"},"Unknown crash boundary"
    assert_unacknowledged(offsets,ack["partition"],ack["offset"])
    inventory=[r for r in snapshot["inventory"] if r["productId"]==event["productId"]]
    receipts=[r for r in snapshot["receipts"] if (r["productId"],r["eventId"])==(event["productId"],event["eventId"])]
    processing=[r for r in snapshot["processing"] if (r["topic"],r["partition"],r["offset"])==(ack["topic"],ack["partition"],ack["offset"])]
    count=0 if stage=="before-commit" else 1
    assert len(inventory)==1 and inventory[0]["balance"]==expected_balance and inventory[0]["ordinal"]==count,"Crash-target inventory differs"
    assert len(receipts)==len(processing)==count,"Crash-target effect/receipt/processing atomicity differs"
    if count:
        assert processing[0]["disposition"]=="APPLIED" and processing[0]["postBalance"]==expected_balance and processing[0]["fingerprint"]==ack["fingerprint"],"Wrong committed target disposition"
        assert receipts[0]["rawKey"]==processing[0]["rawKey"] and receipts[0]["rawValue"]==processing[0]["rawValue"],"Receipt differs from target record"
    return {"stage":stage,"productId":event["productId"],"eventId":event["eventId"],"targetReceipts":len(receipts),"targetProcessingRows":len(processing),"balance":inventory[0]["balance"],"ordinal":inventory[0]["ordinal"],"committedPosition":offsets[str(ack["partition"])],"targetPartition":ack["partition"],"targetOffset":ack["offset"],"snapshotSha256":snapshot_hash(snapshot)}


class Harness:
    """One bounded, namespaced run. Constructing it does not start services."""
    def __init__(self,run_id,output,image_ref,allow_dirty=False,kind="study"):
        self.run_id=run_id;self.output=pathlib.Path(output);self.project="p22_"+run_id;self.owner=owner_labels(run_id);self.started=False;self.followers=[];self.sequence=0;self.deadline=None
        self.report={"kind":kind,"scope":"runtime A2-A7; build/test A1 is recorded by run_checks","runId":run_id,"timezone":"Australia/Brisbane","status":"failed","commands":[],"cases":{name:{"status":"unrun"} for name in ["A2","A3","A4","A5","A6","A7"]}}
        self.records=json.loads((ROOT/"fixtures/events.json").read_text());self.seed=json.loads((ROOT/"fixtures/seed.json").read_text())["balances"];self.expected=json.loads((ROOT/"fixtures/expected.json").read_text());self.fixture_sha=fixture_identity(ROOT/"fixtures")
        filters=["--filter","label=portfolio.project=p22","--filter","label=portfolio.run="+run_id]
        for command in [["docker","ps","-aq",*filters],["docker","volume","ls","-q",*filters],["docker","network","ls","-q",*filters]]:
            if checked(command,5).stdout.strip():raise FileExistsError("Run namespace already has retained resources; choose a new ID")
        if not image_ref:raise ValueError("Set P22_APP_IMAGE to an already-built image")
        image=json.loads(checked(["docker","image","inspect",image_ref],5).stdout)[0];labels=image["Config"].get("Labels",{})
        self.image_id=image["Id"];self.source_revision=labels.get("portfolio.source","");self.jar_sha=labels.get("portfolio.jar-sha256","")
        if labels.get("portfolio.project")!="p22" or not self.source_revision or self.source_revision=="unrecorded" or not re.fullmatch(r"[0-9a-f]{64}",self.jar_sha):raise ValueError("Image lacks verified project/source/JAR metadata")
        head=checked(["git","rev-parse","HEAD"],5).stdout.strip();status=checked(["git","status","--porcelain"],5).stdout.strip()
        if not allow_dirty and (status or self.source_revision!=head):raise ValueError("Canonical runtime acceptance requires a clean source and matching image revision")
        jar=ROOT/"target/retail-event-stream-1.0.0.jar"
        if jar.exists() and hashlib.sha256(jar.read_bytes()).hexdigest()!=self.jar_sha:raise ValueError("Current packaged JAR differs from supplied image metadata")
        self.report.update({"sourceRevision":self.source_revision,"gitHead":head,"gitStatus":status,"imageId":self.image_id,"jarSha256":self.jar_sha,"fixtureSha256":self.fixture_sha,"inputSha256":self.input_hash()})
        self.envfile=self.output/"environment.env"
        values={"P22_RUN_ID":run_id,"P22_CLUSTER_ID":base64.urlsafe_b64encode(uuid.uuid4().bytes).decode().rstrip("="),"P22_DB_PASSWORD":uuid.uuid4().hex,"P22_EVIDENCE_DIR":str(self.output),"P22_APP_IMAGE":self.image_id,"P22_UID":str(os.getuid()),"P22_GID":str(os.getgid())}
        self.envfile.write_text("".join(key+"="+value+"\n" for key,value in values.items()));self.envfile.chmod(0o600)
        self.compose=["docker","compose","--file",str(ROOT/"compose.yaml"),"--env-file",str(self.envfile),"--project-name",self.project]
        self.save()

    def input_hash(self):
        digest=hashlib.sha256();paths=[]
        for directory in ["src/main","fixtures","scripts","tests"]:
            paths.extend(p for p in (ROOT/directory).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
        paths.extend(ROOT/name for name in ["pom.xml","Dockerfile","compose.yaml","toolchain-lock.json"])
        for path in sorted(paths):digest.update(str(path.relative_to(ROOT)).encode()+b"\0"+path.read_bytes())
        return digest.hexdigest()

    def save(self):
        (self.output/"run.json").write_text(json.dumps(self.report,indent=2)+"\n")

    def artifact(self,name,value):
        with (self.output/(name+".json")).open("x") as file:json.dump(value,file,indent=2);file.write("\n")
        return value

    def invoke(self,argv,name,timeout=60,expected=0):
        remaining=timeout if self.deadline is None else min(timeout,self.deadline-time.monotonic()-40)
        self.sequence+=1;path=self.output/(f"{self.sequence:03d}-"+name+".log");start=time.monotonic()
        try:result=run_command(argv,remaining)
        except subprocess.TimeoutExpired as error:
            def text(value):return value.decode(errors="replace") if isinstance(value,bytes) else (value or "")
            path.write_text(text(error.stdout)+text(error.stderr));self.report["commands"].append({"name":name,"exitCode":None,"timeout":True,"elapsedSeconds":round(time.monotonic()-start,3),"log":path.name});self.save();raise
        path.write_text(result.stdout+result.stderr)
        self.report["commands"].append({"name":name,"exitCode":result.returncode,"expectedExitCode":expected,"elapsedSeconds":round(time.monotonic()-start,3),"log":path.name});self.save()
        if result.returncode!=expected:raise RuntimeError(name+" returned "+str(result.returncode)+", expected "+str(expected)+"; log "+path.name)
        return result.stdout

    def app(self,command,scenario,name,projection="live",extra=(),expected=0,timeout=60):
        argv=self.compose+["run","--rm","--no-deps","app",command,"--run-id",self.run_id,"--scenario",scenario,"--manifest","/evidence/"+scenario+"-published.json"]
        if command!="publish":argv += ["--projection",projection]
        return self.invoke(argv+list(extra),name,timeout,expected)

    def start(self):
        self.started=True;self.started_at=time.monotonic();self.deadline=self.started_at+600
        args=["docker","run","--rm","--network","none","--memory","128m"]
        for key,value in {**self.owner,"com.docker.compose.service":"app"}.items():args += ["--label",key+"="+value]
        text=self.invoke(args+["--entrypoint","sha256sum",self.image_id,"/app/app.jar"],"image-jar-identity",15)
        if text.split()[0]!=self.jar_sha:raise AssertionError("Image's actual JAR hash differs")
        self.report["binaryIdentityVerified"]=True
        self.invoke(self.compose+["up","-d","--wait","--wait-timeout","30","kafka","postgres"],"startup",35)
        print(json.dumps({"phase":"started","runId":self.run_id}),flush=True)

    def container(self,service):
        text=self.invoke(self.compose+["ps","-q",service],"locate-"+service,5)
        ids=text.splitlines()
        if len(ids)!=1:raise AssertionError("Expected one owned "+service+" container")
        item=inspect_container(ids[0]);labels=item["Config"].get("Labels",{})
        if any(labels.get(k)!=v for k,v in {**self.owner,"com.docker.compose.service":service}.items()):raise PermissionError("Service ownership differs")
        return ids[0]

    def sql(self,statement,name):
        container_id=self.container("postgres")
        text=self.invoke(["docker","exec",container_id,"psql","-X","-q","-A","-t","-U","p22","-d","p22","-c",statement],name,10)
        return json.loads(text.strip())

    def snapshot(self,scenario,name,projection="live"):
        schema="p22_"+self.run_id+"_"+scenario+"_"+projection
        if not re.fullmatch(r"p22_[a-z0-9_]{1,48}",schema):raise ValueError("Unsafe snapshot identifier")
        q='"'+schema+'"'
        statement=f'''SELECT jsonb_build_object(
          'inventory',(SELECT coalesce(jsonb_agg(to_jsonb(i) ORDER BY i."productId"),'[]'::jsonb) FROM (SELECT product_id AS "productId",balance,(balance<0) AS "negativeStock",ordinal,max_occurred_at AS "maxOccurredAt" FROM {q}.inventory ORDER BY product_id) i),
          'receipts',(SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r."productId",r."eventId"),'[]'::jsonb) FROM (SELECT product_id AS "productId",event_id AS "eventId",replace(encode(raw_key,'base64'),chr(10),'') AS "rawKey",replace(encode(raw_value,'base64'),chr(10),'') AS "rawValue",delta FROM {q}.receipts ORDER BY product_id,event_id) r),
          'processing',(SELECT coalesce(jsonb_agg(to_jsonb(p) ORDER BY p.topic,p.partition,p."offset"),'[]'::jsonb) FROM (SELECT topic,partition_id AS partition,record_offset AS "offset",product_id AS "productId",event_id AS "eventId",replace(encode(raw_key,'base64'),chr(10),'') AS "rawKey",replace(encode(raw_value,'base64'),chr(10),'') AS "rawValue",fingerprint,disposition,reason,late_event AS "lateEvent",post_balance AS "postBalance",ordinal FROM {q}.processing ORDER BY topic,partition_id,record_offset) p));'''
        return self.artifact(name,self.sql(statement,name+"-sql"))

    def transaction_probe(self,scenario,name):
        schema="p22_"+self.run_id+"_"+scenario+"_live"
        if not re.fullmatch(r"p22_[a-z0-9_]{1,48}",schema):raise ValueError("Unsafe transaction probe")
        prefix='insert into "'+schema+'".processing'
        statement="SELECT coalesce(jsonb_agg(jsonb_build_object('pid',pid,'state',state,'hasOpenTransaction',xact_start IS NOT NULL,'queryPrefix',left(query,100))),'[]'::jsonb) FROM pg_stat_activity WHERE datname='p22' AND state='idle in transaction' AND strpos(query,'"+prefix+"')=1;"
        return self.artifact(name,self.sql(statement,name+"-sql"))

    def offsets(self,scenario,name,projection="live"):
        result=json_object_from_output(self.app("offsets",scenario,name,projection),"committed")
        return self.artifact(name,result)

    def publish(self,scenario,sample="full"):
        extra=[] if sample=="full" else ["--sample","retry"]
        self.app("publish",scenario,scenario+"-publish",extra=extra)
        manifest=json.loads((self.output/(scenario+"-published.json")).read_text())
        selected=self.records if sample=="full" else [self.records[2]]
        if manifest["boundary"]["fixtureSha256"]!=self.fixture_sha or len(manifest["records"])!=len(selected):raise AssertionError("Published sample differs from frozen input")
        for ack,record in zip(manifest["records"],selected):
            if ack["index"]!=record["index"] or ack["fingerprint"]!=raw_fingerprint(record):raise AssertionError("Acknowledgement identity differs")
        self.artifact(scenario+"-sample",{"publishedRecords":len(selected),"recordFingerprints":[raw_fingerprint(r) for r in selected],"fixtureSha256":self.fixture_sha})
        return manifest,selected

    def initialize(self,scenario):self.app("init",scenario,scenario+"-init",timeout=30)

    def normal(self):
        manifest,records=self.publish("normal");self.initialize("normal");self.app("consume","normal","normal-consume",timeout=90)
        snapshot=self.snapshot("normal","normal-snapshot");result=verify_snapshot(snapshot,manifest,records,self.expected,self.seed)
        api=json_object_from_output(self.app("snapshot","normal","normal-cli-snapshot"),"inventory")
        if api!=snapshot:raise AssertionError("Packaged CLI snapshot differs from independent SQL")
        positions=self.offsets("normal","normal-offsets")["committed"]
        if positions!=manifest["boundary"]["end"]:raise AssertionError("Normal offsets did not reach frozen end")
        return result

    def crash_case(self,scenario,stage,sample="full",target_index=0):
        manifest,records=self.publish(scenario,sample);self.initialize(scenario)
        record=next(r for r in records if r["index"]==target_index);ack=next(r for r in manifest["records"] if r["index"]==target_index)
        event=json.loads(base64.b64decode(record["valueBase64"]));target=event["productId"]+"/"+event["eventId"]
        name="p22_"+self.run_id+"_"+scenario+"_fault"
        text=self.invoke(self.compose+["run","--detach","--no-deps","--name",name,"app","consume","--run-id",self.run_id,"--scenario",scenario,"--manifest","/evidence/"+scenario+"-published.json","--fault-stage",stage,"--fault-event",target],scenario+"-fault-start",15)
        container_id=text.strip().splitlines()[-1]
        info=inspect_container(container_id)
        log_path=self.output/(scenario+"-fault-consumer.log");file=log_path.open("w")
        follower=subprocess.Popen(["docker","logs","--follow",container_id],stdout=file,stderr=subprocess.STDOUT)
        self.followers.append((follower,file))
        expected={"stage":stage,"runId":self.run_id,"topic":ack["topic"],"partition":ack["partition"],"offset":ack["offset"],"eventId":event["eventId"],"productId":event["productId"],"sourceRevision":self.source_revision,"jarSha256":self.jar_sha}
        marker=await_barrier(log_path,expected,min(60,self.deadline-time.monotonic()-40))
        if not info["Id"].startswith(marker["container"]):raise AssertionError("Barrier names a different container")
        self.artifact(scenario+"-barrier",marker)
        before=self.snapshot(scenario,scenario+"-before-kill");positions=self.offsets(scenario,scenario+"-before-kill-offsets")["committed"]
        balance=self.seed[event["productId"]] if stage=="before-commit" else next(r["postBalance"] for r in self.expected["records"] if r["index"]==target_index)
        before_observation=verify_crash_state(before,positions,ack,event,balance,stage)
        transactions=self.transaction_probe(scenario,scenario+"-before-kill-transactions")
        if stage=="before-commit" and not transactions:raise AssertionError("No staged INSERT/open database transaction observed at precommit barrier")
        if stage=="after-commit" and transactions:raise AssertionError("Database transaction remained open after commit barrier")
        labels={**self.owner,"com.docker.compose.service":"app","portfolio.source":self.source_revision,"portfolio.jar-sha256":self.jar_sha}
        kill_owned_consumer(container_id,labels)
        state=inspect_container(container_id)["State"]
        self.artifact(scenario+"-kill",{"containerId":container_id,"signal":"KILL","exitCode":state["ExitCode"],"oomKilled":state["OOMKilled"],"marker":marker})
        follower.wait(timeout=5);file.close();self.followers.remove((follower,file))
        after=self.snapshot(scenario,scenario+"-after-kill");positions_after=self.offsets(scenario,scenario+"-after-kill-offsets")["committed"]
        after_observation=verify_crash_state(after,positions_after,ack,event,balance,stage)
        if before!=after:raise AssertionError("Committed database state changed across the paused process kill")
        if self.transaction_probe(scenario,scenario+"-after-kill-transactions"):raise AssertionError("Killed consumer's transaction remains open")
        text=self.app("consume",scenario,scenario+"-restart",timeout=90)
        processed=[]
        for line in text.splitlines():
            try:value=json.loads(line)
            except ValueError:continue
            if isinstance(value,dict) and value.get("phase")=="processed" and (value.get("topic"),value.get("partition"),value.get("offset"))==(ack["topic"],ack["partition"],ack["offset"]):processed.append(value)
        if len(processed)!=1 or (stage=="after-commit" and processed[0].get("redelivery") is not True):raise AssertionError("Target redelivery/recovery was not observed")
        final=self.snapshot(scenario,scenario+"-recovered")
        expected_oracle=self.expected if sample=="full" else self.retry_oracle()
        result=verify_snapshot(final,manifest,records,expected_oracle,self.seed)
        return {"publishedRecords":len(records),"killSignal":"KILL","exitCode":137,"before":before_observation,"after":after_observation,"recovery":result,"targetRedelivery":processed[0],"manifest":scenario+"-published.json"}

    def rebuild(self):
        before=self.snapshot("after","rebuild-original-before");positions=self.offsets("after","rebuild-original-offsets-before")["committed"]
        result=json_object_from_output(self.app("rebuild","after","rebuild",projection="rebuild",timeout=90),"inventory")
        rebuilt=self.snapshot("after","rebuild-snapshot",projection="rebuild");after=self.snapshot("after","rebuild-original-after");positions_after=self.offsets("after","rebuild-original-offsets-after")["committed"]
        if rebuilt!=result or before!=after or rebuilt!=before or positions!=positions_after:raise AssertionError("Fresh rebuild or original preservation differs")
        manifest=json.loads((self.output/"after-published.json").read_text());oracle=verify_snapshot(rebuilt,manifest,self.records,self.expected,self.seed)
        return {"readRetainedRecords":22,"originalBeforeSha256":snapshot_hash(before),"originalAfterSha256":snapshot_hash(after),"rebuiltSha256":snapshot_hash(rebuilt),"originalGroupOffsetsUnchanged":True,"projection":oracle}

    def retry_oracle(self):
        return {"counts":{"APPLIED":1},"balances":{"P22_A_5":2,"P22_B_0":10,"P22_C_2":4},"records":[{"index":2,"disposition":"APPLIED","postBalance":4,"ordinal":1,"lateEvent":False}]}

    def retry_cases(self):
        outcomes={};target="P22_C_2/00000000-0000-0000-0000-000000000009"
        for scenario,failures in [("retryok",2),("retryfail",3)]:
            manifest,records=self.publish(scenario,"retry");self.initialize(scenario)
            text=self.app("consume",scenario,scenario+"-consume",extra=["--fail-first",str(failures),"--fault-event",target],expected=0 if failures==2 else 1,timeout=90)
            attempts=[]
            for line in text.splitlines():
                try:value=json.loads(line)
                except ValueError:continue
                if isinstance(value,dict) and value.get("phase")=="attempt":attempts.append(value)
            if [r["attempt"] for r in attempts]!=[1,2,3]:raise AssertionError("Retry budget/attempt evidence differs")
            if failures==3:
                failed=self.snapshot(scenario,scenario+"-failed");positions=self.offsets(scenario,scenario+"-failed-offsets")["committed"]
                event=json.loads(base64.b64decode(records[0]["valueBase64"]))
                verify_crash_state(failed,positions,manifest["records"][0],event,0,"before-commit")
                self.app("consume",scenario,scenario+"-explicit-restart",timeout=90)
            snapshot=self.snapshot(scenario,scenario+"-snapshot")
            result=verify_snapshot(snapshot,manifest,records,self.retry_oracle(),self.seed)
            outcomes[scenario]={"injectedFailures":failures,"attempts":attempts,"explicitRestart":failures==3,"result":result}
        return {"publishedRecords":2,"samples":outcomes,"kind":"injected database exceptions, separate from process kills"}

    def study(self):
        self.start()
        for key,action in [("A2",self.normal),("A3",lambda:self.crash_case("before","before-commit")),("A4",lambda:self.crash_case("after","after-commit")),("A5",self.rebuild),("A6",self.retry_cases)]:
            self.report["cases"][key]={"status":"running"};self.save()
            try:result=action()
            except Exception as error:self.report["cases"][key]={"status":"failed","error":type(error).__name__+": "+str(error)};self.save();raise
            self.report["cases"][key]={"status":"passed",**result};self.save();print(json.dumps({"phase":"case-complete","case":key,"runId":self.run_id}),flush=True)

    def finish(self):
        for process,file in self.followers:
            if process.poll() is None:process.terminate()
            try:process.wait(timeout=3)
            except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
            file.close()
        self.followers=[]
        if self.started:
            try:
                cleanup=cleanup_owned(self.run_id,self.output);self.artifact("cleanup",cleanup);self.report["cases"]["A7"]={"status":"passed" if cleanup["passed"] else "failed",**cleanup}
            except Exception as error:self.report["cases"]["A7"]={"status":"failed","error":str(error)}
            self.report["elapsedSecondsIncludingCleanup"]=round(time.monotonic()-self.started_at,3)
        same_inputs=self.input_hash()==self.report["inputSha256"];self.report["inputFilesUnchanged"]=same_inputs
        required=["smoke","A7"] if self.report["kind"]=="fault-smoke" else ["A2","A3","A4","A5","A6","A7"]
        passed=all(self.report["cases"].get(key,{}).get("status")=="passed" for key in required)
        self.report["status"]="passed" if passed and same_inputs and self.report.get("binaryIdentityVerified") and self.report.get("elapsedSecondsIncludingCleanup",601)<=600 else "failed"
        self.save()
        hashes={str(path.relative_to(self.output)):hashlib.sha256(path.read_bytes()).hexdigest() for path in self.output.rglob("*") if path.is_file() and path.name not in {"environment.env","evidence-sha256.json"}}
        self.artifact("evidence-sha256",hashes)
        return self.report


def main():
    parser=argparse.ArgumentParser();parser.add_argument("--run-id",required=True);parser.add_argument("--output",required=True);args=parser.parse_args()
    owner_labels(args.run_id);output=new_output(args.output,ROOT);harness=None
    try:
        harness=Harness(args.run_id,output,os.environ.get("P22_APP_IMAGE"));harness.study()
    except Exception as error:
        failure={"status":"failed","error":type(error).__name__+": "+str(error)}
        if harness:harness.report["failure"]=failure
        else:(output/"initial-failure.json").write_text(json.dumps(failure,indent=2)+"\n")
        print(json.dumps(failure),flush=True)
    finally:
        report=harness.finish() if harness else {"status":"failed","output":str(output)}
    print(json.dumps({"status":report["status"],"runId":args.run_id,"output":str(output),"elapsedSecondsIncludingCleanup":report.get("elapsedSecondsIncludingCleanup")}),flush=True)
    return 0 if report["status"]=="passed" else 1

if __name__=="__main__":raise SystemExit(main())
