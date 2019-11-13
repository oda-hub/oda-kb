import os
import re
import sys
import io
import glob
import click
import yaml
import hashlib
import traceback
import subprocess
import re
import odakb.sparql as sp
import odakb.datalake as dl

import nb2workflow.nbadapter as nba

def to_bucket_name(n):
    bn = re.sub(r'-+', "-", re.sub(r'\W', "-", n))

    if len(bn)>63:
        bn = bn.replace('gitlab.astro.unige.ch', 'gitlab')
    
    if len(bn)>63:
        bn = hashlib.sha256(bn.encode()).hexdigest()[:8] + "-" + bn[(63-8-1):]

    return bn


def git_get_url(d):
    #git config remote.origin.url
 #   return subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=d).decode().strip()
    return subprocess.check_output(["git", "config", "remote.origin.url"], cwd=d).decode().strip()

def build_local_context():
    context = {}

    # also deduce from gitlhub reference
    for oda_yaml in ["oda.yaml"] + glob.glob("code/*/oda.yaml"):
        if not os.path.exists(oda_yaml): continue

        y = yaml.load(open(oda_yaml))
        print("context: loading oda yaml", oda_yaml, y)
        context[y['uri_base']] = dict()

        if oda_yaml == "oda.yaml":
            context[y['uri_base']]['path']=None
        else:
            context[y['uri_base']]['path']=os.path.dirname(oda_yaml)

        context[y['uri_base']]['version']=subprocess.check_output(["git", "describe", "--always", "--tags", "--dirty"], cwd=context[y['uri_base']]['path']).decode().strip()
        context[y['uri_base']]['origin'] = git_get_url(context[y['uri_base']]['path'])

        context[context[y['uri_base']]['origin']] = context[y['uri_base']]


        

    yaml.dump(context, open("context.yaml", "w"))

    return context

def unique_name(query, kwargs, context):
    s=io.StringIO()
    yaml.dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]
    
    query_alias = query.replace("http://","").replace("/",".") 

    return "{}-{}-{}".format(query_alias, qc, context[query]['version'])
    

def evaluate_local(query, kwargs, context):
    print("will evaluate with local context", context[query])

    nbname_key = kwargs.pop('nbname', 'default')

    fn = "data/{}.yaml".format(unique_name(query, kwargs, context))

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
    if query.startswith("https://gitlab") or query.startswith("git@gitlab"):
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

def git4ci(origin):
    if origin.startswith("git@"):
        origin = origin.replace(":","/").replace("git@","https://")

    if origin.startswith("https://"):
        origin = origin.replace("https://","https://gitlab-ci-token:{}@".format(os.environ.get('CI_JOB_TOKEN','REDACTED')))

    return origin

def fetch_origins(origins, query):
    try:
        base_dir_origin = git_get_url(None)
    except:
        base_dir_origin  = None

    print("fetch origins", origins, "query:", query, "base dir query:", base_dir_origin)

    for origin in origins:

        if origin == base_dir_origin:
            local_copy = None
            return origin, [query, origin]
        else:
            local_copy = os.path.join("code",
                                          re.sub("[:/]", ".", 
                                          re.sub("^https?://", "", query))
                                        )

            try:
                print("trying to clone", origin)
                subprocess.check_call(["git", "clone", origin, local_copy])
                print("clonned succesfully!")
                return origin, [query, origin]
            except: pass

            try:
                print("trying to clone alternative", git4ci(origin))
                subprocess.check_call(["git", "clone", git4ci(origin), local_copy])
                print("clonned succesfully!")
                return git4ci(origin), [query, origin, git4ci(origin)]
            except: pass

            try:
                print("trying to pull")
                subprocess.check_call(["git", "pull"],cwd=local_copy)
                print("managed to  pull")
                return origin, [query]
            except Exception as e: 
                print("git pull failed in",local_copy,"exception:", e)
                continue

    raise Exception("fetching failed, origins: %s"%str(origins))

@click.command()
@click.argument("query")
@sp.unclick
def _evaluate(query, *subqueries, **kwargs):
    cached = kwargs.pop('_cached', True)

    callable_kind, origins = resolve_callable(query)       

    # may also call CWL; verify that CWL is runnable in this container

    print("assuming this container is compliant with", query)
    
    query_fetched_origin, query_names = fetch_origins(origins, query)

    print("fetched origin for query", query, "is", query_fetched_origin, "all query names", query_names)

    context = build_local_context()

    print("got context for", context.keys())

    if query_fetched_origin in context:
        for query_name in query_names:
            context[query_name] = context[query_fetched_origin]
    
    print("after aliasing got context for", context.keys())

    metadata = dict(query=query, kwargs=kwargs, version=context[query]['version'])
    uname = to_bucket_name(unique_name(query, kwargs, context))

    
    
    if cached:
        try:
            d = dl.restore(uname)
            print("got from bucket", uname)
            return d
        except Exception as e:
            traceback.print_exc()
            print("unable to get the bucket:", e)

    # TODO: need construct kwargs from sub queries

    d = None
    if query.startswith("http://") or query.startswith("https://")  or query.startswith("git@"):
        if query in context:
            d = evaluate_local(query, kwargs, context)
        else:
            raise Exception("unable to find query:",query, "have:",context.keys())

    if d is None:
        raise Exception("unable to interpret query")

    try:
        dl.store(d, metadata, uname)
    except Exception as e:
        print("problem storing to the datalake", e)

    return d

            


if __name__ == "__main__":
    _evaluate()
            
    
