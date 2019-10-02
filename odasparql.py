import requests
import os


def create():
    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/update',
                   data='''PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX da: <https://www.wowman.org/index.php?id=1&type=get#>
    prefix dda: <http://ddahub.io/ontology/analysis#>

    INSERT DATA
    { 
      dda:planning_advice dda:describes "planning"                       
    }''',
        auth=requests.auth.HTTPBasicAuth("admin", open(os.path.join(os.environ.get('HOME'), '.jena-password')).read().strip())
    )


    print(r)

    print(r.text)

def get_planning():
    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/query',
                   params=dict(query='''PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    prefix dda: <http://ddahub.io/ontology/analysis#>

    SELECT ?s where
    { 
      ?s dda:describes "planning"                       
    }''')
    )


    print(r)

    print(r.text)


#create()
get_planning()

