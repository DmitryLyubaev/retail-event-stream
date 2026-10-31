"""Stop and remove only containers/networks with this run's verified ownership labels."""
import argparse
import json
import pathlib

if __package__:
    from .run_support import checked, inspect_container, owner_labels
else:
    from run_support import checked, inspect_container, owner_labels

def cleanup_owned(run_id, output=None):
    expected=owner_labels(run_id)
    report={"runId":run_id,"containers":[],"refused":[],"errors":[]}
    filters=["--filter","label=portfolio.project=p22","--filter","label=portfolio.run="+run_id]
    ids=checked(["docker","ps","-aq",*filters],5).stdout.splitlines()
    owned=[];running=[]
    for container_id in ids:
        item=inspect_container(container_id);labels=item["Config"].get("Labels",{})
        if any(labels.get(k)!=v for k,v in expected.items()) or labels.get("com.docker.compose.service") not in {"app","kafka","postgres"}:
            report["refused"].append(container_id);continue
        owned.append(container_id)
        if item["State"]["Running"]:running.append(container_id)
        report["containers"].append({"id":item["Id"],"service":labels.get("com.docker.compose.service"),"image":item["Config"].get("Image"),"state":{key:item["State"].get(key) for key in ["Running","ExitCode","OOMKilled","Pid","StartedAt","FinishedAt","Error"]}})
        if output:
            try:
                logs=checked(["docker","logs",container_id],5)
                (pathlib.Path(output)/("container-"+container_id+".log")).write_text(logs.stdout+logs.stderr)
            except Exception as error:
                report["errors"].append("logs: "+str(error))
    if running:
        try:checked(["docker","stop","--time","5",*running],10)
        except Exception as error:report["errors"].append("stop: "+str(error))
    if owned:
        try:checked(["docker","rm",*owned],10)
        except Exception as error:report["errors"].append("remove: "+str(error))
    networks=checked(["docker","network","ls","-q",*filters],5).stdout.splitlines()
    for network_id in networks:
        item=json.loads(checked(["docker","network","inspect",network_id],5).stdout)[0]
        if any(item.get("Labels",{}).get(k)!=v for k,v in expected.items()) or item.get("Containers"):
            report["refused"].append(network_id);continue
        try:checked(["docker","network","rm",network_id],5)
        except Exception as error:report["errors"].append("network: "+str(error))
    report["retainedVolumes"]=checked(["docker","volume","ls",*filters,"--format","{{.Name}}"],5).stdout.splitlines()
    report["remainingOwnedContainers"]=checked(["docker","ps","-q",*filters],5).stdout.splitlines()
    report["passed"]=not report["errors"] and not report["refused"] and not report["remainingOwnedContainers"]
    return report

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--run-id",required=True);args=parser.parse_args()
    report=cleanup_owned(args.run_id);print(json.dumps(report,indent=2));return 0 if report["passed"] else 1

if __name__=="__main__":raise SystemExit(main())
