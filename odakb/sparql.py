import copy
import os
import re
import io
import json
import pprint
import time
import glob
import sys
import yaml
import rdflib
import pkg_resources
import requests
import importlib

import keyring
import click
import logging

from os import getenv
from keyrings.cryptfile.cryptfile import CryptFileKeyring
kr = CryptFileKeyring()
kr.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD") or None 
keyring.set_keyring(kr)

keyring.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD", None) 


#logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("odakb.sparql")
#logger.setLevel(logging.INFO)

def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    logger.setLevel(level)


default_prefixes=[]
default_graphs=[]

# TODO: allow to produce local context, also offline

G = rdflib.Graph()
    

def dump_loggers():
    for k,v in  logging.Logger.manager.loggerDict.items()  :
        print('+ [%s] {%s} ' % (str.ljust( k, 20)  , str(v.__class__)[8:-2]) ) 
        if not isinstance(v, logging.PlaceHolder):
            for h in v.handlers:
                print('     +++',str(h.__class__)[8:-2] )

def set_silent():
    setup_logging(logging.ERROR)

def set_debug():
    setup_logging(logging.DEBUG)
    

def parse_shortcuts(graph_serial):
    g = graph_serial.replace("="," an:equalTo ")
    if not g.strip().endswith("."):
        g += " ."

    return g

def load_graph(G, serial, shortcuts=False):
    if serial.startswith("https://"):
        G.load(serial)
    else:
        logger.info("will load:", serial,"INFO")
        G.parse(
            data="\n".join(default_prefixes) + (lambda x:x if not shortcuts else parse_shortcuts)(serial),
            format="turtle"
        )


def load_defaults(default_prefixes, default_graphs):

    for odakb_defaults in [
                os.path.join("/etc/odakb/defaults.yaml"),
                os.path.join(getenv("HOME"), ".odakb", "defaults.yaml"),
            ]:
        try:
            logger.info("oda defaults from %s", odakb_defaults)

            for p in yaml.safe_load(open(odakb_defaults))['prefixes']:
                if p not in default_prefixes:
                    logger.info("appending new prefix: %s", p)
                    default_prefixes.append(p)
        except Exception as e:
            logger.info("unable to load default prefixes from %s: %s", odakb_defaults, repr(e))
    
    try:
        odakb_defaults_http = "http://ontology.odahub.io/defaults/defaults.yaml"
        logger.info("oda defaults from %s", odakb_defaults_http)

        for p in yaml.safe_load(io.StringIO(requests.get(odakb_defaults_http).text))['prefixes']:
            if p not in default_prefixes:
                logger.info("appending new prefix: %s", p)
                default_prefixes.append(p)
    except Exception as e:
        logger.info("unable to load default prefixes: %s", repr(e))
        raise

    try:
        odakb_graphs = glob.glob(os.path.join(getenv("HOME"), ".odakb", "graphs.d","*"))
        logger.info("default graphs from: %s", odakb_graphs)
        for oda_graph_fn in odakb_graphs:
            default_graphs.append(open(oda_graph_fn).read())
    except Exception as e:
        logger.info("unable to load default graphs: %s", e)


def process_graph_loaders(G):
    for default_graph in default_graphs:
        logger.info("loading default graph", default_graph)
        load_graph(G, default_graph)

    q = """
        SELECT ?loader ?rm ?location WHERE {
            ?loader a oda:graphLoader;
                    oda:runMethod ?rm;
                    oda:location ?location .
        }
        """
    r = query(q)
        
    for loader, rm, loc in G.query(q):
        logger.info(loader, rm, loc)
        if str(rm) == "http://odahub.io/ontology#pythonModule":
            logger.info("will load python module", loc)
            mn, fn = loc.split(".")
            m = importlib.import_module(mn)
            getattr(m, fn)(G)
        else:
            raise Exception("unable to exploit run method %s"%rm)


def init():
    load_defaults(default_prefixes, default_graphs)

query_stats = None


def unclick(f):
    """aliases function as _function without click's decorators"""
    if f.__name__.startswith('_'):
        setattr(sys.modules[f.__module__], f.__name__[1:], f)
    return f


@click.group()
@click.option("-d", "--debug", is_flag=True, default=False)
@click.option("-q", "--quiet", is_flag=True, default=False)
@click.option("-p", "--prefixes", default=None)
def cli(debug, quiet, prefixes):
    if debug and quiet:
        raise Exception("can not be quiet and debug!")

    if debug:
        setup_logging(logging.DEBUG)
        logger.error('test erro')
        logger.info('test erro')
    elif quiet:
        setup_logging(logging.ERROR)
    else:
        setup_logging()


def reset_stats_collection():
    global query_stats
    query_stats=[]

def stop_stats_collection():
    global query_stats
    query_stats=None

def note_stats(**kwargs):
    global query_stats

    if query_stats is None:
        query_stats = []

    query_stats.append(kwargs)

def report_stats():
    logger.info(query_stats)

    if query_stats is not None:
        summary=dict(
            n_queries=len(query_stats),
            spent_seconds=sum([s['spent_seconds'] for s in query_stats]),
            spent_longest_seconds=max([s['spent_seconds'] for s in query_stats]),
            query_size=sum([s['spent_seconds'] for s in query_stats]),
        )

        return summary

class SPARQLException(Exception):
    pass

def get_jena_password():
    tried = []

    for n, m in {
                'environ': lambda:os.environ['JENA_PASSWORD'].strip(),
                'keyring': lambda:keyring.get_password("jena", "admin"),
            }.items():
        try:
            r = m()
            print("good JENA password from", n)
            return r
        except Exception as e:
            print("failed", n, m, e)
            tried.append([n, m, e])

    raise RuntimeError("no good jena password, tried: "+repr(tried))

def compose_sparql(body, prefixes=None):
    _prefixes = copy.deepcopy(default_prefixes)
    if prefixes is not None:
        _prefixes += prefixes

    return "\n".join(_prefixes)+"\n\n"+body


def execute_sparql(data, endpoint, invalid_raise):
    logger.debug("data: %s", repr(data))
        

    t0=time.time()

    oda_sparql_root = os.environ.get("ODA_SPARQL_ROOT", "https://sparql.odahub.io/dataanalysis")

    if endpoint == "update":    
        auth=requests.auth.HTTPBasicAuth("admin", 
                                         get_jena_password())
        r=requests.post(oda_sparql_root+"/"+endpoint,
                        data=data,
                        auth=auth
                        )
    else:
        auth=None
        r=requests.post(oda_sparql_root+"/"+endpoint,
                       params=dict(query=data)
                    )

    note_stats(spent_seconds=time.time()-t0, query_size=len(data))
    
    logger.debug(r)
    logger.debug(r.text)
    logger.debug(report_stats())

    
    if invalid_raise:
        if r.status_code not in [200, 201, 204]:
            raise SPARQLException(r.status_code, r.text)

    try:
        return r.json()
    except:
        return {'problem-decoding': r.text}

@cli.command("update")
@click.argument("query")
@unclick
def _update(query, invalid_raise=True):
    data = compose_sparql(query)

    return execute_sparql(data, 'update', invalid_raise=invalid_raise)

@cli.command("insert")
@click.argument("query")
#@click.pass_context
@unclick
def _insert(query=None):
    return update(query="INSERT DATA {\n" + query  + "\n}")

def create(entries):
    
    return update("INSERT DATA {\n" + ("\n".join(["%s %s %s ."%t3 for t3 in entries])) + "\n}")

def query(query, invalid_raise=True):
    data = compose_sparql(query)

    return execute_sparql(data, 'query', invalid_raise=invalid_raise)

@cli.command("select")
@click.argument("query")
@click.argument("form", required=False, default=None)
@click.option("-j", "--json", "tojson", is_flag=True)
@click.option("-r", "--rdf", "tordf", is_flag=True)
@unclick
def _select(query=None, form=None, todict=True, tojson=False, tordf=False):
    init()

    if form is None:
        form = query

    data = compose_sparql("SELECT * WHERE {\n" + query + "\n}")

    r = execute_sparql(data, 'query', invalid_raise=True)
    entries = [ { k: v['value'] for k, v in _r.items() } for _r in r['results']['bindings'] ]

    if tordf or tojson:
        rdf = "\n".join(default_prefixes)
        rdf += "\n\n" + "\n".join([render_rdf(form, e)+" ." for e in entries])

    if tordf:
        print(rdf)
        return rdf

    if tojson:
        g = rdflib.Graph().parse(data=rdf, format='turtle') 
        jsonld = g.serialize(format='json-ld', indent=4, sort_keys=True).decode()
        print(jsonld)
        return jsonld


    if todict:
        return entries 
    else:
        return r

class ManyAnswers(Exception):
    pass

class NoAnswers(Exception):
    pass

@cli.command("select-one")
@click.argument("query")
@unclick
def _select_one(query=None):
    r = select(query)

    if len(r) > 1:
        raise ManyAnswers(r)
    
    if len(r) == 0:
        raise NoAnswers() 

    r = r[0]

    return r

def render_uri(uri, entry=None):
    if entry is None:
        entry={}

    r = uri

    if uri.startswith("?"):
        r = entry[uri[1:]]

    if r.startswith("http"):
        r = "<%s>"%r

    if not any([r.startswith(p) for p in ["<", "oda:", "data:"]]):
        r = "\"%s\""%r

    return r

def render_rdf(query, entry):
    s, p, o = map(lambda u: render_uri(u, entry), query.split())

    return "%s %s %s"%(s, p, o)

@cli.command("delete")
@click.argument("query")
@click.argument("fact", required=False)
@click.option("-a", '--all-entries', default=False)
@click.option("-n", default=10)
@unclick
def _delete(query=None, fact=None, todict=True, all_entries=False, n=10):
    if not a:
        data = compose_sparql("DELETE DATA {\n" + query + "\n}")

        r = execute_sparql(data, 'update',  debug=debug, invalid_raise=True)
    else:
        entries = select(query)

        print("found entries to delete:\n")

        if fact is None:
            fact = query

        rdf_es = []
        for entry in entries:
            print(entry)

            rdf = render_rdf(fact, entry)
            print("rdf", rdf)

            rdf_es.append(rdf)

        if len(rdf_es)<=n:
            print("deleting...")

            data = compose_sparql("DELETE DATA {\n" + " .\n".join(rdf_es) + "\n}")
            r = execute_sparql(data, 'update',  debug=debug, invalid_raise=True)
        else:
            print("refusing to delete %i > %i entries"%(len(rdf_es), n))


@cli.command("reason")
@click.argument("query")
@click.argument("fact")
@click.option("-c/-nc",'--commit/--no-commit', default=False)
@unclick
def _reason(query, fact, commit=False):
    new_facts = []

    for r in select(query):
        print("applying", r, "to fact", fact)

        for k, v in r.items():
            fact = re.sub(r"\?%s\b"%k, "<%s>"%v, fact)

        new_facts.append(fact)

        print("new fact:", fact)

        if commit:
            insert(fact)

    return new_facts

@cli.command()
def version():
    click.echo(pkg_resources.get_distribution("oda-knowledge-base").version)

if __name__ == "__main__":
    cli()
