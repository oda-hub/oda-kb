import os
import re
import sys
import io
import glob
import copy
import yaml
import pprint
import hashlib
import logging
import argparse
import traceback
import importlib
import subprocess
import pkg_resources
import odakb.sparql as sp
import odakb.datalake as dl

import numpy as np # type: ignore

logger = logging.getLogger("odakb.sparql")


def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    logger.setLevel(level)

def set_silent():
    setup_logging(logging.ERROR)

def set_debug():
    setup_logging(logging.DEBUG)

try:
    import nb2workflow.nbadapter as nba # type: ignore
except Exception as e:
    logger.debug("unable to import evaluator for nba! %s", e)


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
    bn = re.sub(r'[\W_]', "-", n)
    bn = re.sub(r'-+', "-", bn)
    bn = re.sub(r'^[^a-z]', "b", bn)

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

    logger.debug("local context will discover code locally, possibly already fetched and ready for execution")

    # also deduce from gitlhub reference
    for oda_yaml in ["oda.yaml"] + glob.glob("code/*/oda.yaml"):

        oda_yaml = os.path.realpath(oda_yaml)

        if not os.path.exists(oda_yaml):
            continue
        
        logger.info("found oda.yaml: %s", oda_yaml)

        try:
            y = yaml.safe_load(open(oda_yaml))
        except:
            y = yaml.load(open(oda_yaml))

        logger.debug("context: loading oda yaml %s %s", oda_yaml, y)
        context[y['uri_base']] = dict()

        if oda_yaml == "oda.yaml":
            context[y['uri_base']]['path']=None
        else:
            context[y['uri_base']]['path']=os.path.dirname(oda_yaml)

        context[y['uri_base']]['version']=subprocess.check_output(["git", "describe", "--always", "--tags", "--dirty"], cwd=context[y['uri_base']]['path']).decode().strip()
        context[y['uri_base']]['origin'] = git_get_url(context[y['uri_base']]['path'])
        context[y['uri_base']]['callable_kind'] = callable_kind
        
        context[y['uri_base']]['oda'] = y

        context[context[y['uri_base']]['origin']] = context[y['uri_base']]

       # for origin in origins:
        #    context[origin] = context[y['uri_base']]


    if callable_kind == "http://odahub.io/ontology#pypi-function":

        if len(origins) != 1:
            raise Exception("one and only one origin required; found: {}".format(origins))

        origin = origins[0]

        package_name, package_callable = origin.split(".", 1)
        logger.debug("found pypi-based function %s %s", package_name, package_callable)


        context[query] = dict()
        context[query]['origin'] = package_name
        context[query]['path'] = package_callable
        context[query]['version'] = pkg_resources.get_distribution(package_name).version
        context[query]['callable_kind'] = callable_kind
        

    yaml.safe_dump(context, open("context.yaml", "w"))

    return context

def unique_name(query, args, kwargs, context):
    logger.debug("unique name for %s %s context: %s", query, kwargs, context)

    s=io.StringIO()
    yaml.safe_dump(args, s)
    yaml.safe_dump(kwargs, s)
    qc = hashlib.sha256(s.getvalue().encode()).hexdigest()[:8]
    
    query_alias = query.replace("http://","").replace("/","_") 

    r="{}-{}-{}-{}".format(query_alias, kwargs.get('nbname', 'default'), context[query]['version'], qc)
    logger.debug("unique name is %s for %s, %s, context %s", r, query,kwargs,context)
    return r
    

def execute_local(query, args, kwargs, context):
    logger.info("\033[31mwill execute\033[0m (evaluate with local context)")

    for k,v in context[query].items():
        logger.info("\033[34m%20s: %s\033[0m", k, v)

    logger.debug("full query: %s kwargs %s", query, kwargs)
    
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

            logger.debug("nbnames: %s", nbnames)
            
            if nbname_key == "default":
                if context[query]['oda'].get("root_notebook", None) is not None:
                    nbname = os.path.join(
                                context[query]['path'],
                                context[query]['oda']['root_notebook']
                             )
                    logger.info("using default nbname %s", nbname)
                else:
                    logger.debug("no default nbname")
                    if len(nbnames) == 1:
                        nbname = nbnames[0]
                        logger.debug("using only available nbname %s", nbname)
                    else:
                        raise Exception("one and only one nb possible with default key, found {}".format(nbnames))
            else:
                nbname = nbdir + "/" + nbname_key + ".ipynb"
        
            logger.debug("nbrun with %s %s", nbname, kwargs)
            d = nba.nbrun(nbname, kwargs)

            logger.debug("nbrun returns:")

            for k, v in d.items():
                if not k.endswith("_content"):
                    logger.debug("%s: %s", k, pprint.pformat(v))
        
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
    logger.info("\033[33mrequested to resolve callable %s\033[0m", query)

    if query.startswith("https://gitlab") or query.startswith("git@gitlab"):
        logger.info("\033[32mdirect query to gitlab\033[0m")
        return "http://odahub.io/callable/notebook", [query], query
    
    if not query.startswith("http://") and not query.startswith("https://") \
       and os.path.isdir(query):
        logger.info("\033[35massuming directory is prioritized and considered notebook!\033[0m")
        query = os.path.realpath(query)

        oda_yaml_fn = os.path.join(query, "oda.yaml")
        try:
            oda_yaml = yaml.load(open(oda_yaml_fn), Loader=yaml.SafeLoader)
        except IOError as e:
            logger.error("unable to read oda.yaml from callable directory %s, %s", oda_yaml_fn, e)
            raise

        canonical_query = oda_yaml['uri_base']

        logger.info("\033[32massuming %s directory is our callable\033[0m", query)
        return "http://odahub.io/callable/notebook", [query], canonical_query

    try:
        r = sp.select("<%s> oda:callableKind ?kind ."%query)
        logger.debug(r)
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
    except Exception as e:
        logger.exception("\033[31mproblem finding locations: %s\033[0", e)
        r=[]
    
    logger.info("found locations: %s", r)

    locations = r

    canonical_queries = [ L['location'] for L  in locations if L['location'].startswith("http://odahub.io") ]
    
    if len(canonical_queries) == 1:
        canonical_query = canonical_queries[0]
    elif len(canonical_queries) > 1:
        canonical_query = canonical_queries[0]
        logger.warning("\033[31msuspiciously many canonical query names! %s; will use first one\033[0m", canonical_queries)

    else:
        canonical_query = query
        logger.warning("\033[31mno actual canonical query will default %s\033[0m", canonical_query)
    
    return callable_kind, [l['location'] for l in locations], canonical_query

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

    logger.info("fetch_origins '%s' for query '%s' base dir query '%s'", origins, query, base_dir_origin)

    for origin in origins:

        if origin == base_dir_origin:
            local_copy = None
            return origin, [query, origin]
        else:
            local_copy = os.path.join("code",
                                      re.sub("[:/\.]", "_", 
                                             re.sub("^https?://", "", query),
                                            )
                                     )

            logger.info("local copy: %s", os.path.realpath(local_copy))

            for _origin in origin, git4ci(origin):
                re_origin = os.environ.get('ODAKB_ALLOWED_ORIGINS', '.*')
                if re.match(re_origin, _origin):
                    logger.info("origin %s allowed by %s", _origin, re_origin)
                else:
                    logger.info("\033[31morigin %s FORBIDDEN by %s\033[0m", _origin, re_origin)
                    continue

                logger.info("trying to clone %s as %s", origin, _origin)
                try:
                    subprocess.check_call(["git", "clone", _origin, local_copy])
                    logger.info("\033[32mclonned succesfully!\033[0m")
                    return _origin, [query, origin, _origin]
                except Exception as e:
                    logger.info("\033[31mfailed to clone: %s!\033[0m", e)

            try:
                logger.debug("trying to pull")
                subprocess.check_call(["git", "pull"],cwd=local_copy)
                logger.info("managed to pull")
                return origin, [query, origin]
            except Exception as e: 
                logger.debug("git pull failed in",local_copy,"exception:", e)
                continue

    return None, []


def evaluate(query=None, *args, **kwargs):
    restrict_execution_modes = kwargs.pop('restrict', "local,baobab") # None means all

    restrict_execution_modes = restrict_execution_modes.split(",")

    for execution_mode in restrict_execution_modes:
        try:
            logger.debug("trying execution mode", execution_mode)

            r_func=globals()['evaluate_'+execution_mode]

            logger.debug("execution mode will proceed", execution_mode, "with", r_func)

            logger.debug("towards evaluation")
            return r_func(query, *args, **kwargs)
        except Exception as e:
            logger.debug("failed mode", execution_mode, e)
            raise 




def evaluate_local(query, *args, **kwargs):
    cached = kwargs.pop('_cached', True)
    write_files = kwargs.pop('_write_files', False)
    return_metadata = kwargs.pop('_return_metadata', False)

    if query.endswith(".ipynb"):
        query, nb = query.rsplit("/", 1)
        kwargs['nbname'] = nb.replace(".ipynb", "")
        logger.info("detected notebook in query: putting it in kwargs, remaining %s and %s", query, nb)


    callable_kind, origins, canonical_query = resolve_callable(query)       

    logger.info("\033[32mresolved callable:\033[0m \033[36m\n -- kind: %s\n -- origins: %s\n -- canonical_query: %s\033[0m", 
                    callable_kind, origins, canonical_query)

    # may also call CWL; verify that CWL is runnable in this container

    logger.warning("assuming this environment is compliant with %s", query)
    
    query_fetched_origin, query_names = fetch_origins(origins, callable_kind, query)

    logger.info("\033[33mfetched origin for query %s is %s, all query names %s\033[0m", query, query_fetched_origin, query_names)

    context = build_local_context(query, origins, callable_kind)

    logger.info("complete local context contains:")

    for k,v in context.items():
        logger.info("\033[36m -- %50s: %s\033[0m", k, v['path'])

    if query_fetched_origin in context:
        for query_name in query_names:
            context[query_name] = context[query_fetched_origin]
    
    logger.debug("after aliasing got context for", context.keys())

    if query in context:
        metadata = dict(query=query, kwargs=kwargs, version=context[query]['version'])
    elif canonical_query in context:
        metadata = dict(query=query, kwargs=kwargs, version=context[canonical_query]['version'])
    else:
        raise Exception(f" requested query '{query}' or '{canonical_query}' not found in context, have {'; '.join(list(context.keys()))}")

    uname = to_bucket_name(unique_name(query, args, kwargs, context)) # unique-name contains version
    
    logger.info("\033[31mbucket name %s\033[0m", uname)
    
    if cached:
        try:
            d = dl.restore(uname, write_files=write_files, return_metadata=return_metadata)
            logger.info("\033[32mrestored from bucket %s\033[0m", uname)
            return d
        except Exception as e:
            logger.debug("unable to get the bucket", uname, ":", e)

    # TODO: need construct kwargs from sub queries

    d = None

    query = canonical_query

    if query.startswith("http://") or query.startswith("https://")  or query.startswith("git@"):
        if query in context:
            d = execute_local(query, args, kwargs, context)
        else:
            raise Exception(f"unable to find query: {query} have: {context.keys()}")
    else:
        raise Exception(f"unable to execute query {query} since it is not http")

    if d is None:
        raise Exception("unable to interpret query")

    try:
        r=dl.store(d, metadata, uname)
        logger.info("\033[32msuccessfully stored to the datalake %s\033[0m", r)
    except Exception as e:
        logger.error("\033[31mproblem storing to the datalake: %s\033[0m", e)

    if return_metadata:
        return metadata, d
    else:
        return d

            
def main():
    parser = argparse.ArgumentParser(description='Run/evaluate/call some callable/evaluatable/runnable things') # run locally, remotely, semantically
    parser.add_argument('query', metavar='query', type=str)
    parser.add_argument('--debug', action="store_true")
    parser.add_argument('--quiet', action="store_true", default=False)
    parser.add_argument('-r', metavar='restrict', type=str)
    parser.add_argument('inputs', nargs=argparse.REMAINDER)

    args = parser.parse_args()

    inputs={}
    k = None
    for i in args.inputs:
        if k is not None:
            v = i
            inputs[k] = v
            logger.info("found value for key: %s: %s", k, v)
            k = None
        else:
            if i.startswith("--inp-"):
                k = i.replace('--inp-','')
                logger.info("found key: %s", k)
            else:
                logger.warning("parameter cound not be interpretted %s", i)
        
    if args.debug:
        setup_logging(logging.DEBUG)
        logger.error('test error')
        logger.info('test error')
    elif args.quiet:
        setup_logging(logging.ERROR)
    else:
        setup_logging()


    evaluate(args.query, **inputs)

            
if __name__ == "__main__":
    main()
