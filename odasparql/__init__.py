import requests
import copy
import os

default_prefixes=[
    "PREFIX owl: <http://www.w3.org/2002/07/owl#>",
    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>",
    "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>",
    "PREFIX da: <https://www.wowman.org/index.php?id=1&type=get#>",
    "PREFIX dda: <http://ddahub.io/ontology/analysis#>",
    "PREFIX tns: <http://odahub.io/ontology/tns#>",
]

def compose_sparql(body, prefixes=None):
    _prefixes = copy.deepcopy(default_prefixes)
    if prefixes is not None:
        _prefixes += prefixes

    return "\n".join(_prefixes)+"\n\n"+body

def create(entries, prefixes=None, debug=True):
    data = compose_sparql("INSERT DATA {" + ("\n".join(["%s %s %s"%t3 for t3 in entries])) + "}", prefixes)

    if debug:
        print("data:", data)

    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/update',
                   data=data,
        auth=requests.auth.HTTPBasicAuth("admin", open(os.path.join(os.environ.get('HOME'), '.jena-password')).read().strip())
    )

    print(r)
    print(r.text)


def query(query, prefixes=None, debug=True):
    _query = compose_sparql(query)

    if debug:
        print("sending queru:\n", _query)

    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/query',
                   params=dict(query=_query)
                )

    print(r)
    print(r.text)

    return r.json()

if __name__ == "__main__":
    query("""
    SELECT ?s where
    { 
      ?s dda:describes "planning"                       
    }
    """)
