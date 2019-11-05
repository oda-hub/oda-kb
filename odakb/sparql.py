import copy
import os
import sys
import time

import requests

import keyring
import click



default_prefixes=[
    "PREFIX owl: <http://www.w3.org/2002/07/owl#>",
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "PREFIX da: <https://www.wowman.org/index.php?id=1&type=get#>",
    "PREFIX dda: <http://ddahub.io/ontology/analysis#>",
    "PREFIX data: <http://ddahub.io/ontology/data#>",
    "PREFIX tns: <http://odahub.io/ontology/tns#>",
    "PREFIX oda: <http://odahub.io/ontology#>",
    "PREFIX foaf: <http://xmlns.com/foaf/0.1/>"
]

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
    print(query_stats)

    if query_stats is not None:
        summary=dict(
            queries=query_stats,
            spent_seconds=sum([s['spent_seconds'] for s in query_stats]),
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
        print("data:", data)
        
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
        print(r)
        print(r.text)

        print(report_stats())

    
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

if __name__ == "__main__":
    cli()
