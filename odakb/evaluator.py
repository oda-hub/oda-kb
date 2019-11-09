import os
import sys
import io
import glob
import click
import yaml
import hashlib
import subprocess
import re
import odakb.sparql as sp

import nb2workflow.nbadapter as nba

def build_local_context():
    context = {}

    # also deduce from gitlhub reference
    for oda_yaml in glob.glob("code/*/oda.yaml"):
        y = yaml.load(open(oda_yaml))
        print("loading oda yaml", oda_yaml, y)
        context[y['uri_base']] = dict(path=os.path.dirname(oda_yaml))
        context[y['uri_base']]['version']=subprocess.check_output(["git", "describe", "--always", "--tags", "--dirty"], cwd=context[y['uri_base']]['path']).decode().strip()

    yaml.dump(context, open("context.yaml", "w"))

    return context


def evaluate_local(query, kwargs, context):
    query_alias = query.replace("http://","").replace("/",".") 

    print("will evaluate with local context", context[query])

    s=io.StringIO()
    yaml.dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]

    fn = "data/{}-{}-{}.yaml".format(query_alias, qc, context[query]['version'])

    if os.path.exists(fn):
        d=yaml.load(open(fn))

    else:
        nbnames = glob.glob(context[query]['path'])
        assert len(nbnames) == 1
        nbname = nbnames[0]
    
        print("nbrun with",nbname, kwargs)
        d = nba.nbrun(nbname, kwargs)

        print("nbrun returns",d)

        for k, v in d.items():
            try:
                d[k] = yaml.load(v)
            except: pass

        try:
            os.makedirs("data")
        except:
            pass
    
        yaml.dump(d, open(fn, "w"))

    return d

def resolve_callable(query):
    r=sp.select(None, "<%s> oda:callableKind ?kind ."%query)
    print(r)
    
    if r['results']['bindings'] == []:
        callable_kind="http://odahub.io/callable/notebook"

    r=sp.select(None, "<%s> oda:location ?location ."%query)
    
    print(r)

    locations = r['results']['bindings']
    
    return callable_kind, [l['location']['value'] for l in locations]

def fetch_location(locations, query):
    for location in locations:
        local_location = os.path.join("code",
                                      re.sub("[:/]", ".", 
                                      re.sub("^https?://", "", query))
                                    )

        try:
            subprocess.check_call(["git", "clone", location, local_location])
            return
        except: pass

        try:
            subprocess.check_call(["git", "pull"],cwd=local_location)
            return
        except: 
            continue

        return 
    raise Exception("fetching failed")

@click.command()
@click.argument("query")
@sp.unclick
def _evaluate(query, *subqueries, **kwargs):
    callable_kind, locations = resolve_callable(query)       

    # may also call CWL; verify that CWL is runnable in this container

    print("assuming this container is compliant with", query)

    fetch_location(locations, query)

    context = build_local_context()

    # construct kwargs from sub queries

    if query.startswith("http://"):
        if query in context:
            d = evaluate_local(query, kwargs, context)
            return d
    
    raise Exception("unable to interpret query")

            


if __name__ == "__main__":
    _evaluate()
            
    
