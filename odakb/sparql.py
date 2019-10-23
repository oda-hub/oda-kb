import requests
import copy
import os

import time


default_prefixes=[
    "PREFIX owl: <http://www.w3.org/2002/07/owl#>",
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "PREFIX da: <https://www.wowman.org/index.php?id=1&type=get#>",
    "PREFIX dda: <http://ddahub.io/ontology/analysis#>",
    "PREFIX data: <http://ddahub.io/ontology/data#>",
    "PREFIX tns: <http://odahub.io/ontology/tns#>",
    "PREFIX oda: <http://odahub.io/ontology#>",
]

query_stats = None

def start_stats_collection():
    query_stats=[]

def stop_stats_collection():
    query_stats=None

def note_stats(**kwargs):
    if query_stats is not None:
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

start_stats_collection()

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
        auth=requests.auth.HTTPBasicAuth("admin", open(os.path.join(os.environ.get('HOME'), '.jena-password')).read().strip())
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

    
    if invalid_raise:
        if r.status_code not in [200, 201, 204]:
            raise SPARQLException(r.status_code, r.text)

    try:
        return r.json()
    except:
        return {'problem-decoding': r.text}

def update(query, prefixes=None, debug=True, invalid_raise=True):
    data = compose_sparql(query, prefixes)

    return execute_sparql(data, 'update', debug=debug, invalid_raise=invalid_raise)

def create(entries, prefixes=None, debug=True):
    return update("INSERT DATA {\n" + ("\n".join(["%s %s %s ."%t3 for t3 in entries])) + "\n}", prefixes)


def query(query, prefixes=None, debug=True, invalid_raise=True):
    data = compose_sparql(query, prefixes)

    return execute_sparql(data, 'query',  debug=debug, invalid_raise=invalid_raise)

if __name__ == "__main__":
    query("""
    SELECT ?s where
    { 
      ?s dda:describes "planning"                       
    }
    """)
