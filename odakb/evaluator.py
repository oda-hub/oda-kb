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
    for oda_yaml in ["oda.yaml"] + glob.glob("code/*/oda.yaml"):
        if not os.path.exists(oda_yaml): continue

        y = yaml.load(open(oda_yaml))
        print("loading oda yaml", oda_yaml, y)
        context[y['uri_base']] = dict()

        if oda_yaml == "oda.yaml":
            context[y['uri_base']]['path']=None
        else:
            context[y['uri_base']]['path']=os.path.dirname(oda_yaml)

        print("running in", context[y['uri_base']]['path'])
    
        context[y['uri_base']]['version']=subprocess.check_output(["git", "describe", "--always", "--tags", "--dirty"], cwd=context[y['uri_base']]['path']).decode().strip()
        context[y['uri_base']]['origin'] = subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=context[y['uri_base']]['path']).decode().strip()

        context[context[y['uri_base']]['origin']] = context[y['uri_base']]


        

    yaml.dump(context, open("context.yaml", "w"))

    return context


def evaluate_local(query, kwargs, context):
    query_alias = query.replace("http://","").replace("/",".") 


    print("will evaluate with local context", context[query])

    s=io.StringIO()
    yaml.dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]
    
    nbname_key = kwargs.pop('nbname', 'default')

    fn = "data/{}-{}-{}.yaml".format(query_alias, qc, context[query]['version'])

    if os.path.exists(fn):
        d=yaml.load(open(fn))

    else:
        if context[query]['path'] is None:
            nbdir = "./"
        else:
            nbdir = context[query]['path']

        nbnames = glob.glob(nbdir+"/*ipynb")

        print("nbnames:", nbnames)
        
        if nbname_key == "default":
            assert len(nbnames) == 1
            nbname = nbnames[0]
        else:
            nbname = nbdir + "/" + nbname_key + ".ipynb"
    
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
    if query.startswith("https://gitlab"):
        print("direct query to gitlab")
        return "http://odahub.io/callable/notebook", [query]

    r=sp.select(None, "<%s> oda:callableKind ?kind ."%query)
    print(r)
    
    if r['results']['bindings'] == []:
        callable_kind="http://odahub.io/callable/notebook"

    r=sp.select(None, "<%s> oda:location ?location ."%query)
    
    print(r)

    locations = r['results']['bindings']
    
    return callable_kind, [l['location']['value'] for l in locations]

def fetch_origins(origins, query):
    try:
        base_dir_origin = subprocess.check_output(["git", "remote", "get-url", "origin"]).decode().strip()
    except:
        base_dir_origin  = None

    for origin in origins:

        if origin == base_dir_origin:
            local_copy = None
        else:
            local_copy = os.path.join("code",
                                          re.sub("[:/]", ".", 
                                          re.sub("^https?://", "", query))
                                        )

            try:
                subprocess.check_call(["git", "clone", origin, local_copy])
                return
            except: pass

            try:
                subprocess.check_call(["git", "pull"],cwd=local_copy)
                return
            except: 
                continue

        return 
    raise Exception("fetching failed")

@click.command()
@click.argument("query")
@sp.unclick
def _evaluate(query, *subqueries, **kwargs):
    callable_kind, origins = resolve_callable(query)       

    # may also call CWL; verify that CWL is runnable in this container

    print("assuming this container is compliant with", query)
    
    fetch_origins(origins, query)

    context = build_local_context()

    # construct kwargs from sub queries

    if query.startswith("http://") or query.startswith("https://"):
        if query in context:
            d = evaluate_local(query, kwargs, context)
            return d
        else:
            raise Exception("unable to find query:",query, "have:",context.keys())
    
    raise Exception("unable to interpret query")

            


if __name__ == "__main__":
    _evaluate()
            
    
