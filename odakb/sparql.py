import copy
import os
import re
import io
import time
import glob
import sys
import yaml
import pkg_resources
import requests
import importlib

import keyring
import click

from os import getenv
from keyrings.cryptfile.cryptfile import CryptFileKeyring
kr = CryptFileKeyring()
kr.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD") or None 
keyring.set_keyring(kr)

keyring.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD", None) 

import logging

logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


default_prefixes=[]

default_graphs=[]

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


load_defaults(default_prefixes, default_graphs)

query_stats = None


def unclick(f):
    """aliases function as _function without click's decorators"""
    if f.__name__.startswith('_'):
        setattr(sys.modules[f.__module__], f.__name__[1:], f)
    return f


@click.group()
def cli():
    pass

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


def execute_sparql(data, endpoint, debug, invalid_raise):
    if debug:
        logger.info("data: %s", repr(data))
        

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
    
    if debug:
        logger.info(r)
        logger.info(r.text)

        logger.info(report_stats())

    
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
def _update(query, prefixes=None, debug=True, invalid_raise=True):
    data = compose_sparql(query, prefixes)

    return execute_sparql(data, 'update', debug=debug, invalid_raise=invalid_raise)

@cli.command("insert")
@click.argument("query")
#@click.pass_context
@unclick
def _insert(query=None, prefixes=None, debug=True):
    #return ctx.invoke(update, query="INSERT DATA {\n" + query  + "\n}", prefixes=prefixes)
    return update(query="INSERT DATA {\n" + query  + "\n}", prefixes=prefixes)

def create(entries, prefixes=None, debug=True):
    return update("INSERT DATA {\n" + ("\n".join(["%s %s %s ."%t3 for t3 in entries])) + "\n}", prefixes)

def query(query, prefixes=None, debug=True, invalid_raise=True):
    data = compose_sparql(query, prefixes)

    return execute_sparql(data, 'query',  debug=debug, invalid_raise=invalid_raise)

@cli.command("select")
@click.argument("query")
@unclick
def _select(query=None, prefixes=None, debug=True, todict=True):
    data = compose_sparql("SELECT * WHERE {\n" + query + "\n}", prefixes)

    r = execute_sparql(data, 'query',  debug=debug, invalid_raise=True)

    if todict:
        return [ { k: v['value'] for k, v in _r.items() } for _r in r['results']['bindings'] ]
    else:
        return r

class ManyAnswers(Exception):
    pass

class NoAnswers(Exception):
    pass

@cli.command("select-one")
@click.argument("query")
@unclick
def _select_one(query=None, prefixes=None, debug=True):
    r = select(query)

    if len(r) > 1:
        raise ManyAnswers(r)
    
    if len(r) == 0:
        raise NoAnswers() 

    r = r[0]

    return r

def render_uri(uri, entry):
    r = uri

    if uri.startswith("?"):
        r = entry[uri[1:]]

    if r.startswith("http"):
        r = "<%s>"%r

    if not (r.startswith("oda:") or r.startswith("<")):
        r = "\"%s\""%r

    return r

def render_rdf(query, entry):
    s, p, o = map(lambda u: render_uri(u, entry), query.split())

    return "%s %s %s"%(s, p, o)

@cli.command("delete")
@click.argument("query")
@click.option("-a/-na", default=False)
@click.option("-n", default=10)
@unclick
def _delete(query=None, prefixes=None, debug=True, todict=True, a=False, n=10):
    if not a:
        data = compose_sparql("DELETE DATA {\n" + query + "\n}", prefixes)

        r = execute_sparql(data, 'update',  debug=debug, invalid_raise=True)
    else:
        entries = select(query)

        print("found entries to delete:\n")

        rdf_es = []
        for entry in entries:
            print(entry)
            
            rdf = render_rdf(query, entry)
            print("rdf", rdf)

            rdf_es.append(rdf)

        if len(rdf_es)<=n:
            print("deleting...")

            data = compose_sparql("DELETE DATA {\n" + " .\n".join(rdf_es) + "\n}", prefixes)
            r = execute_sparql(data, 'update',  debug=debug, invalid_raise=True)
        else:
            print("refusing to delete %i > %i entries"%(len(rdf_es), n))


@cli.command("reason")
@click.argument("query")
@click.argument("fact")
@click.option("--commit/--no-commit", default=False)
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
