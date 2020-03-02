import copy
import os
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


import logging
logger = logging.getLogger(__name__)

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
    try:
        odakb_defaults = os.path.join(os.environ.get("HOME"), ".odakb", "defaults.yaml")
        logger.info("oda defaults from %s", odakb_defaults)

        for p in yaml.safe_load(open(odakb_defaults))['prefixes']:
            if p not in default_prefixes:
                logger.info("appending new prefix: %s", p)
                default_prefixes.append(p)
    except Exception as e:
        logger.info("unable to load default prefixes:", e)
    
    try:
        odakb_defaults_http = "http://ontology.odahub.io/defaults/defaults.yaml"
        logger.info("oda defaults from %s", odakb_defaults_http)

        for p in yaml.safe_load(io.StringIO(requests.get(odakb_defaults_http).text))['prefixes']:
            if p not in default_prefixes:
                logger.info("appending new prefix: %s", p)
                default_prefixes.append(p)
    except Exception as e:
        logger.info("unable to load default prefixes:", e)
        raise

    try:
        odakb_graphs = glob.glob(os.path.join(os.environ.get("HOME"), ".odakb", "graphs.d","*"))
        logger.info("default graphs from: %s", odakb_graphs)
        for oda_graph_fn in odakb_graphs:
            default_graphs.append(open(oda_graph_fn).read())
    except Exception as e:
        logger.info("unable to load default graphs: %s", e)


def process_graph_loaders(G):
    for default_graph in default_graphs:
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

def compose_sparql(body, prefixes=None):
    _prefixes = copy.deepcopy(default_prefixes)
    if prefixes is not None:
        _prefixes += prefixes

    return "\n".join(_prefixes)+"\n\n"+body


def execute_sparql(data, endpoint, debug, invalid_raise):
    if debug:
        logger.info("data:", data)
        
    if endpoint == "update":    
        auth=requests.auth.HTTPBasicAuth("admin", 
                                         keyring.get_password("jena", "admin"))
    else:
        auth=None

    t0=time.time()

    if endpoint == "update":    
        r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/'+endpoint,
                        data=data,
                        auth=auth
                        )
    else:
        r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/'+endpoint,
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
@click.pass_context
@unclick
def _insert(ctx=None, query=None, prefixes=None, debug=True):
    return ctx.invoke(update, query="INSERT DATA {\n" + query  + "\n}", prefixes=prefixes)

def create(entries, prefixes=None, debug=True):
    return update("INSERT DATA {\n" + ("\n".join(["%s %s %s ."%t3 for t3 in entries])) + "\n}", prefixes)

def query(query, prefixes=None, debug=True, invalid_raise=True):
    data = compose_sparql(query, prefixes)

    return execute_sparql(data, 'query',  debug=debug, invalid_raise=invalid_raise)

@cli.command("select")
@click.argument("query")
@click.pass_context
@unclick
def _select(ctx=None, query=None, prefixes=None, debug=True):
    data = compose_sparql("SELECT * WHERE {\n" + query + "\n}", prefixes)

    return execute_sparql(data, 'query',  debug=debug, invalid_raise=True)

@cli.command()
def version():
    click.echo(pkg_resources.get_distribution("oda-knowledge-base").version)

if __name__ == "__main__":
    cli()
