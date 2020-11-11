import os
import re
import sys
import io
import glob
import copy
import click
import yaml
import pprint
import hashlib
import traceback
import importlib
import subprocess
import pkg_resources
import odakb.sparql as sp
import odakb.datalake as dl

import numpy as np # type: ignore

try:
    import nb2workflow.nbadapter as nba # type: ignore
except Exception as e:
    print("unable to import evaluator for nba!", e)


# evaluation is reification
# enerative workflows are reificationo
# facts and rules

def placeholder(*a, **aa):
    raise Exception("this should be replaced in runtime")

def add_numpy_representers():

    def numpy_representer_seq(dumper, data):
        return dumper.represent_sequence('!ndarray:', data.tolist())
    
    def numpy_representer_int64(dumper, data):
        return dumper.represent_scalar('!numpy.int64:', "%i"%int(data))
    
    def numpy_representer_float64(dumper, data):
        return dumper.represent_scalar('!numpy.float64:', "%.20lg"%float(data))

    #yaml.add_representer(np.ndarray, numpy_representer_str)
    yaml.SafeDumper.add_representer(np.ndarray, numpy_representer_seq)
    yaml.SafeDumper.add_representer(np.int64, numpy_representer_int64)
    yaml.SafeDumper.add_representer(np.float64, numpy_representer_float64)
    #yaml.add_representer(unicode, str_presenter)
    
    yaml.SafeLoader.add_constructor('!ndarray:', numpy_representer_seq)
    yaml.SafeLoader.add_constructor('!numpy.int64:', lambda self, node: np.int64(node.value))
    yaml.SafeLoader.add_constructor('!numpy.float64:', lambda self, node: np.float64(node.value))

    print (yaml.dump({'a':np.arange(4)},default_flow_style=False))


def to_bucket_name(n):
    bn = re.sub(r'-+', "-", re.sub(r'\W', "-", n))

    if len(bn)>63:
        bn = bn.replace('gitlab.astro.unige.ch', 'gitlab')
    
    if len(bn)>63:
        bn = hashlib.sha256(bn.encode()).hexdigest()[:8] + "-" + bn[(63-8-1):]

    bn = bn.lower()

    return bn


def git_get_url(d):
    #git config remote.origin.url
 #   return subprocess.check_output(["git", "remote", "get-url", "origin"], cwd=d).decode().strip()
    return subprocess.check_output(["git", "config", "remote.origin.url"], cwd=d).decode().strip()

def build_local_context(query, origins, callable_kind):
    context = {}

    # also deduce from gitlhub reference
    for oda_yaml in ["oda.yaml"] + glob.glob("code/*/oda.yaml"):
        if not os.path.exists(oda_yaml): continue

        try:
            y = yaml.safe_load(open(oda_yaml))
        except:
            y = yaml.load(open(oda_yaml))

        print("context: loading oda yaml", oda_yaml, y)
        context[y['uri_base']] = dict()

        if oda_yaml == "oda.yaml":
            context[y['uri_base']]['path']=None
        else:
            context[y['uri_base']]['path']=os.path.dirname(oda_yaml)

        context[y['uri_base']]['version']=subprocess.check_output(["git", "describe", "--always", "--tags", "--dirty"], cwd=context[y['uri_base']]['path']).decode().strip()
        context[y['uri_base']]['origin'] = git_get_url(context[y['uri_base']]['path'])
        context[y['uri_base']]['callable_kind'] = callable_kind

        context[context[y['uri_base']]['origin']] = context[y['uri_base']]

    if callable_kind == "http://odahub.io/ontology#pypi-function":

        if len(origins) != 1:
            raise Exception("one and only one origin required; found: {}".format(origins))

        origin = origins[0]

        package_name, package_callable = origin.split(".", 1)
        print("found pypi-based function", package_name, package_callable)


        context[query] = dict()
        context[query]['origin'] = package_name
        context[query]['path'] = package_callable
        context[query]['version'] = pkg_resources.get_distribution(package_name).version
        context[query]['callable_kind'] = callable_kind
        

    yaml.safe_dump(context, open("context.yaml", "w"))

    return context

def unique_name(query, args, kwargs, context):
    print("unique name for", query, kwargs, context)

    s=io.StringIO()
    yaml.safe_dump(args, s)
    yaml.safe_dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]
    
    query_alias = query.replace("http://","").replace("/",".") 

    r="{}-{}-{}-{}".format(query_alias, kwargs.get('nbname', 'default'), context[query]['version'], qc)
    print("unique name is", r, "for", query,kwargs,context)
    return r
    

def execute_local(query, args, kwargs, context):
    print("will evaluate with local context", context[query])
    print("full query:", query, "kwargs", kwargs)
    
    fn = "data/{}.yaml".format(unique_name(query, args, kwargs, context))

    kwargs = copy.deepcopy(kwargs)
    nbname_key = kwargs.pop('nbname', 'default')

    callable_kind = context[query].get('callable_kind')

    if os.path.exists(fn):
        try:
            d=yaml.safe_load(open(fn))
        except:
            d=yaml.load(open(fn))

    else:
        if callable_kind == "http://odahub.io/ontology#pypi-function":
            module = importlib.import_module(context[query]['origin'])
            func = getattr(module, context[query]['path'])
            d = func(*args, **kwargs)
        else:
            if context[query]['path'] is None:
                nbdir = "./"
            else:
                nbdir = context[query]['path']

            nbnames = [n for n in glob.glob(nbdir+"/*ipynb") if not n.endswith("_output.ipynb")]

            print("nbnames:", nbnames)
            
            if nbname_key == "default":
                if len(nbnames) == 1:
                    nbname = nbnames[0]
                else:
                    raise Exception("one and only one nb possible with default key, found {}".format(nbnames))
            else:
                nbname = nbdir + "/" + nbname_key + ".ipynb"
        
            print("nbrun with",nbname, kwargs)
            d = nba.nbrun(nbname, kwargs)

            print("nbrun returns:")

            for k, v in d.items():
                if not k.endswith("_content"):
                    print(k, pprint.pformat(v))
        
                try:
                    d[k] = yaml.safe_load(v)
                except:
                    pass

        try:
            os.makedirs("data")
        except:
            pass
    
        yaml.safe_dump(d, open(fn, "w"))

    return d

def resolve_callable(query):
    if query.startswith("https://gitlab") or query.startswith("git@gitlab"):
        print("direct query to gitlab")
        return "http://odahub.io/callable/notebook", [query]

    try:
        r = sp.select("<%s> oda:callableKind ?kind ."%query)
        print(r)
    except:
        r = []
    
    if r == []:
        callable_kind="http://odahub.io/callable/notebook"
    elif len(r) == 1:
        callable_kind=r[0]['kind']
    else:
        raise Exception("multiple callable: %s"%repr(r['results']['bindings']))

    try:
        r=sp.select("<%s> oda:location ?location ."%query)
    except:
        r=[]
    
    print(r)

    locations = r
    
    return callable_kind, [l['location'] for l in locations]

def git4ci(origin):
    if origin.startswith("git@"):
        origin = origin.replace(":","/").replace("git@","https://")

    if origin.startswith("https://"):
        origin = origin.replace("https://","https://gitlab-ci-token:{}@".format(os.environ.get('CI_JOB_TOKEN','REDACTED')))

    return origin

def fetch_origins(origins, callable_kind, query):
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
                return origin, [query, origin]
            except Exception as e: 
                print("git pull failed in",local_copy,"exception:", e)
                continue

    return None, []


#def query_args_kwargs2rdf(query, *args, **kwargs):
#    return


evaluate = placeholder

@click.command()
@click.argument("query")
@click.option("-r","--restrict")
@sp.unclick
def _evaluate(query=None, *args, **kwargs):
    restrict_execution_modes = kwargs.pop('restrict', "local,baobab") # None means all

    #Q = query_args_kwargs2rdf(query, *args, **kwargs)

    restrict_execution_modes = restrict_execution_modes.split(",")

    for execution_mode in restrict_execution_modes:
        try:
            print("trying execution mode", execution_mode)

            r_func=globals()['evaluate_'+execution_mode]

            print("execution mode will proceed", execution_mode, "with", r_func)

            print("towards evaluation")
            return r_func(query, *args, **kwargs)
        except Exception as e:
            print("failed mode", execution_mode, e)
            raise 



def evaluate_local(query, *args, **kwargs):
    cached = kwargs.pop('_cached', True)
    write_files = kwargs.pop('_write_files', False)

    callable_kind, origins = resolve_callable(query)       

    # may also call CWL; verify that CWL is runnable in this container

    print("assuming this container is compliant with", query)
    
    query_fetched_origin, query_names = fetch_origins(origins, callable_kind, query)

    print("fetched origin for query", query, "is", query_fetched_origin, "all query names", query_names)

    context = build_local_context(query, origins, callable_kind)

    print("got context for", context.keys())

    if query_fetched_origin in context:
        for query_name in query_names:
            context[query_name] = context[query_fetched_origin]
    
    print("after aliasing got context for", context.keys())

    metadata = dict(query=query, kwargs=kwargs, version=context[query]['version'])
    uname = to_bucket_name(unique_name(query, args, kwargs, context)) # unique-name contains version

    
    
    if cached:
        try:
            d = dl.restore(uname, write_files=write_files)
            print("got from bucket", uname)
            return d
        except Exception as e:
            print("unable to get the bucket", uname, ":", e)

    # TODO: need construct kwargs from sub queries

    d = None

    if query.startswith("http://") or query.startswith("https://")  or query.startswith("git@"):
        if query in context:
            d = execute_local(query, args, kwargs, context)
        else:
            raise Exception("unable to find query:",query, "have:",context.keys())

    if d is None:
        raise Exception("unable to interpret query")

    try:
        r=dl.store(d, metadata, uname)
        print("successfully stored to the datalake", r)
    except Exception as e:
        print("problem storing to the datalake", e)

    return d

            


if __name__ == "__main__":
    _evaluate()
            
    
