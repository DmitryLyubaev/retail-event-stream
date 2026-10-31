"""Bounded process, identity and evidence helpers. No global Docker operations."""
import base64
import hashlib
import json
import pathlib
import re
import struct
import subprocess
import time

ROOT=pathlib.Path(__file__).resolve().parents[1]

def run_command(argv: list[str], timeout_seconds: float) -> subprocess.CompletedProcess:
    if timeout_seconds<=0: raise TimeoutError("Command budget exhausted")
    return subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout_seconds,check=False)

def checked(argv, timeout_seconds=10):
    result=run_command(argv,timeout_seconds)
    if result.returncode: raise subprocess.CalledProcessError(result.returncode,argv,result.stdout,result.stderr)
    return result

def await_barrier(log_path: pathlib.Path, expected: dict, timeout_seconds: float) -> dict:
    deadline=time.monotonic()+timeout_seconds
    while True:
        if log_path.exists():
            text=log_path.read_text(errors="replace")
            for line in text.splitlines(keepends=True):
                if not line.endswith("\n"): continue
                try: value=json.loads(line)
                except ValueError: continue
                if isinstance(value,dict) and value.get("phase")=="fault-barrier":
                    if any(value.get(key)!=wanted for key,wanted in expected.items()): raise ValueError("Fault barrier identity/stage mismatch")
                    if not isinstance(value.get("pid"),int) or value["pid"]<1 or not re.fullmatch(r"[0-9a-f]{12,64}",str(value.get("container",""))): raise ValueError("Fault barrier lacks process/container identity")
                    return value
        remaining=deadline-time.monotonic()
        if remaining<=0: raise TimeoutError("Fault barrier not observed within the bound")
        time.sleep(min(0.025,remaining))

def owner_labels(run_id):
    if not re.fullmatch(r"r[0-9a-f]{12}",run_id): raise ValueError("Unsafe run ID")
    return {"portfolio.project":"p22","portfolio.run":run_id,"com.docker.compose.project":"p22_"+run_id}

def inspect_container(container_id):
    if not re.fullmatch(r"[0-9a-f]{12,64}",container_id): raise PermissionError("Require explicit container ID")
    value=json.loads(checked(["docker","inspect",container_id],5).stdout)
    if not isinstance(value,list) or len(value)!=1 or not value[0].get("Id","").startswith(container_id): raise PermissionError("Container identity mismatch")
    return value[0]

def kill_owned_consumer(container_id: str, expected_labels: dict) -> None:
    required={"portfolio.project","portfolio.run","com.docker.compose.project","com.docker.compose.service"}
    if not required.issubset(expected_labels) or expected_labels["portfolio.project"]!="p22" or expected_labels["com.docker.compose.service"]!="app": raise PermissionError("Incomplete consumer ownership expectation")
    if any(expected_labels.get(key)!=value for key,value in owner_labels(expected_labels["portfolio.run"]).items()): raise PermissionError("Inconsistent ownership expectation")
    before=inspect_container(container_id)
    labels=before.get("Config",{}).get("Labels",{})
    if any(labels.get(key)!=value for key,value in expected_labels.items()) or not before["State"]["Running"] or not before["Config"].get("Cmd") or before["Config"]["Cmd"][0]!="consume": raise PermissionError("Refusing to kill a non-owned consumer")
    checked(["docker","kill","--signal","KILL",container_id],5)
    after=inspect_container(container_id)
    if after["State"]["Running"] or after["State"].get("ExitCode")!=137 or after["State"].get("OOMKilled"): raise AssertionError("Intentional abrupt termination was not verified")

def assert_unacknowledged(offsets, partition, offset):
    if set(offsets)!={"0","1","2"}: raise AssertionError("Missing group offset observations")
    position=offsets[str(partition)]
    if position is not None and (not isinstance(position,int) or position<0 or position>offset): raise AssertionError("Kafka offset already passed the crash target")

def new_output(path, workspace):
    workspace=pathlib.Path(workspace).resolve();path=pathlib.Path(path)
    if not path.is_absolute(): path=workspace/path
    path=path.resolve();allowed=(workspace/"artifacts").resolve()
    if allowed.parent!=workspace or path==allowed or allowed not in path.parents: raise ValueError("Evidence must be a new directory inside this project's artifacts")
    if path.exists(): raise FileExistsError("Refusing to overwrite existing evidence")
    path.mkdir(parents=True,exist_ok=False)
    return path

def raw_fingerprint(record):
    digest=hashlib.sha256()
    for name in ["keyBase64","valueBase64"]:
        raw=None if record[name] is None else base64.b64decode(record[name],validate=True)
        digest.update(struct.pack(">i",-1 if raw is None else len(raw)))
        if raw is not None: digest.update(raw)
    return digest.hexdigest()

def fixture_identity(directory):
    digest=hashlib.sha256()
    for name in ["seed.json","events.json","expected.json"]:
        raw=(directory/name).read_bytes();digest.update(struct.pack(">q",len(raw)));digest.update(raw)
    return digest.hexdigest()

def json_object_from_output(text, required_key):
    found=[]
    for line in text.splitlines():
        try: value=json.loads(line)
        except ValueError: continue
        if isinstance(value,dict) and required_key in value: found.append(value)
    if len(found)!=1: raise AssertionError("Expected exactly one JSON observation containing "+required_key)
    return found[0]
