"""Canonical local verification and source-bound packaged acceptance."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET

if __package__:
    from .run_support import ROOT,checked,run_command,new_output,owner_labels,fixture_identity,json_object_from_output
    from .demo import Harness
else:
    from run_support import ROOT,checked,run_command,new_output,owner_labels,fixture_identity,json_object_from_output
    from demo import Harness

def junit_counts(directory):
    reports=sorted(Path(directory).glob("TEST-*.xml"))
    if not reports:raise ValueError("No actual JUnit reports")
    totals={key:0 for key in ["tests","failures","errors","skipped"]}
    for path in reports:
        node=ET.parse(path).getroot()
        for key in totals:totals[key]+=int(node.attrib[key])
    if totals["tests"]<=0 or any(totals[key] for key in ["failures","errors","skipped"]):raise ValueError("JUnit acceptance refused: "+str(totals))
    return totals

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--run-id",required=True);args=parser.parse_args();owner_labels(args.run_id)
    os.chdir(ROOT)
    if checked(["git","status","--porcelain"],5).stdout.strip():raise ValueError("Commit source before canonical verification")
    source=checked(["git","rev-parse","HEAD"],5).stdout.strip()
    out=new_output("artifacts/"+args.run_id+"-verification",ROOT)
    report={"runId":args.run_id,"sourceRevision":source,"dateUtc":datetime.datetime.now(datetime.timezone.utc).isoformat(),"status":"failed","cases":{"A1":{"status":"running"},**{key:{"status":"unrun"} for key in ["A2","A3","A4","A5","A6","A7"]}},"commands":[]}
    def save(): (out/"acceptance.json").write_text(json.dumps(report,indent=2)+"\n")
    def run(argv,name,timeout):
        start=time.monotonic()
        try:result=run_command(argv,timeout)
        except subprocess.TimeoutExpired as error:
            def text(value):return value.decode(errors="replace") if isinstance(value,bytes) else (value or "")
            (out/(name+".log")).write_text(text(error.stdout)+text(error.stderr));report["commands"].append({"name":name,"timeout":True,"elapsedSeconds":round(time.monotonic()-start,3)});save();raise
        (out/(name+".log")).write_text(result.stdout+result.stderr)
        report["commands"].append({"name":name,"exitCode":result.returncode,"elapsedSeconds":round(time.monotonic()-start,3)});save()
        if result.returncode:raise RuntimeError(name+" failed, see "+str(out/(name+".log")))
        return result.stdout+result.stderr
    start=time.monotonic();harness=None;save()
    try:
        paths=checked(["git","ls-files","-z"],5).stdout.split("\0")
        hashes={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in paths if name}
        (out/"source-files-sha256.json").write_text(json.dumps(hashes,indent=2)+"\n")
        report["sourceTreeSha256"]=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(",",":")).encode()).hexdigest()
        report["fixtureSha256"]=fixture_identity(ROOT/"fixtures");report["gitStatus"]=[];report["platform"]={"system":platform.platform(),"python":sys.version}
        run(["docker","version","--format","{{json .}}"],"docker-version",10)
        run(["docker","compose","version"],"compose-version",10)
        run(["docker","info","--format",'{{json .MemTotal}} {{json .NCPU}} {{json .Architecture}}'],"engine-capacity",10)
        run(["docker","ps","--format","{{.Names}} {{.Image}}"],"shared-host-containers",10)
        run(["bash","scripts/check.sh","--version"],"builder-version",60)
        container_out="/workspace/"+str(out.relative_to(ROOT))
        run(["bash","scripts/check.sh","spotless:check","clean","verify","org.apache.maven.plugins:maven-dependency-plugin:3.10.0:tree","-DoutputFile="+container_out+"/dependency-tree.txt","org.apache.maven.plugins:maven-help-plugin:3.5.2:effective-pom","-Doutput="+container_out+"/effective-pom.xml"],"clean-verification",1200)
        counts={}
        for kind,folder in [("unit","surefire-reports"),("integration","failsafe-reports")]:
            counts[kind]=junit_counts(ROOT/"target"/folder);shutil.copytree(ROOT/"target"/folder,out/folder)
        python=run([sys.executable,"-m","unittest","discover","-s","tests","-v"],"python-verification",60)
        match=re.search(r"Ran (\d+) tests? in",python)
        if not match or int(match.group(1))<=0 or "skipped=" in python or not re.search(r"\nOK\s*$",python):raise ValueError("Python checks were absent, skipped or unsuccessful")
        counts["python"]={"tests":int(match.group(1)),"failures":0,"errors":0,"skipped":0}
        jar=ROOT/"target/retail-event-stream-1.0.0.jar";jar_sha=hashlib.sha256(jar.read_bytes()).hexdigest()
        tag="p22-retail-stream:"+args.run_id
        run(["docker","build","--build-arg","P22_SOURCE_REVISION="+source,"--build-arg","P22_JAR_SHA256="+jar_sha,"-t",tag,"."],"runtime-image-build",300)
        image=checked(["docker","image","inspect","--format","{{.Id}}",tag],5).stdout.strip()
        (out/"runtime-image-inspect.json").write_text(checked(["docker","image","inspect",image],5).stdout)
        run(["docker","run","--rm","--network","none","--memory","256m","--entrypoint","java",image,"-version"],"runtime-java-version",20)
        validated=json_object_from_output(run(["docker","run","--rm","--network","none","--memory","384m",image,"validate","--fixture","fixtures"],"packaged-validate",30),"validatedRecords")
        if validated!={"validatedRecords":22,"fixtureSha256":report["fixtureSha256"]}:raise AssertionError("Packaged validator differs from independent fixture identity")
        if checked(["git","status","--porcelain"],5).stdout.strip() or checked(["git","rev-parse","HEAD"],5).stdout.strip()!=source:raise AssertionError("Source changed during verification")
        report.update({"jarSha256":jar_sha,"imageId":image,"preparationSeconds":round(time.monotonic()-start,3)})
        report["cases"]["A1"]={"status":"passed","checks":counts,"packagedValidator":validated};save()
        print(json.dumps({"phase":"A1-complete","runId":args.run_id,"checks":counts}),flush=True)
        study=new_output("artifacts/"+args.run_id+"-study",ROOT)
        harness=Harness(args.run_id,study,image)
        try:harness.study()
        except Exception as error:harness.report["failure"]={"error":type(error).__name__+": "+str(error)};raise
        finally:
            runtime=harness.finish();report["cases"].update(runtime["cases"]);report["runtimeReport"]=str(study.relative_to(ROOT)/"run.json");report["elapsedSecondsIncludingCleanup"]=runtime.get("elapsedSecondsIncludingCleanup");report["runtimeStatus"]=runtime["status"];save()
        report["status"]="passed" if runtime["status"]=="passed" and all(case["status"]=="passed" for case in report["cases"].values()) else "failed"
    except Exception as error:
        report["failure"]={"error":type(error).__name__+": "+str(error)}
        if report["cases"]["A1"]["status"]=="running":report["cases"]["A1"]["status"]="failed"
    finally:
        save();hashes={str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest() for p in out.rglob("*") if p.is_file() and p.name!="evidence-sha256.json"};(out/"evidence-sha256.json").write_text(json.dumps(hashes,indent=2)+"\n")
    print(json.dumps({"status":report["status"],"runId":args.run_id,"output":str(out),"failure":report.get("failure")}),flush=True)
    return 0 if report["status"]=="passed" else 1

if __name__=="__main__":raise SystemExit(main())
