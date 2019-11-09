import os
import io
import glob
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
        context[y['uri_base']] = os.path.dirname(oda_yaml)

    return context


def evaluate_local(query, kwargs, context):
    query_alias = query.replace("http://","").replace("/",".") 

    print("will evaluate with local context", context[query])

    s=io.StringIO()
    yaml.dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]

    fn = "data/{}-{}.yaml".format(query_alias, qc)

    if os.path.exists(fn):
        d=yaml.load(open(fn))

    else:
        nbnames = glob.glob(context[query])
        assert len(nbnames) == 1
        nbname = nbnames[0]
    
        d = nba.nbrun(nbname, kwargs)

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

    r=sp.select(None, "<%s> oda:location ?kind ."%query)
    
    print(r)

    locations = r['results']['bindings']
    assert len(locations) == 1
    location= locations[0]
    
    return callable_kind, location['kind']['value']

def fetch_location(location, query):
    local_location = os.path.join("code",
                                  re.sub("^http", "", 
                                  re.sub("[:/]", "", query))
                                )

    try:
        subprocess.check_call(["git", "clone", location, local_location])
    except:
        subprocess.check_call(["git", "pull"],cwd=local_location)

def evaluate(query, *subqueries, **kwargs):
    callable_kind, location = resolve_callable(query)       

    if location.startswith("https"):
        fetch_location(location, query)

    context = build_local_context()

    # construct kwargs from sub queries

    if query.startswith("http://"):


        if query in context:
            d = evaluate_local(query, kwargs, context)

    return d
            

    
    
