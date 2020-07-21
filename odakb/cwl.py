import cwltool.factory # type: ignore
import requests
import os
import json
import io
import ast

from minio import Minio # type: ignore
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, # type: ignore
             BucketAlreadyExists)

def get_minio():
    return Minio('minio-internal.odahub.io',
              access_key='minio',
              secret_key=open(os.environ.get('HOME')+"/.minio").read().strip(),
              secure=False)

def run(inputs):
    fac = cwltool.factory.Factory()

    tool = fac.make("crab.cwl")

    result = tool(
        **inputs
    )



    result_json=json.dumps(result)

    print(result_json)

    return result_json, open("crab.cwl","r").read()

def load(bucket):
    client = get_minio()
    try:
        result = client.get_object(bucket, 'result')
        inputs = client.get_object(bucket, 'inputs')
        meta = json.loads(client.get_object(bucket, 'meta').read())
    except Exception as e:
        raise

    return result, meta['cwl']


def store(meta, inputs, result_json, bucket_name):

    # Initialize client with an endpoint and access/secret keys.
    client = get_minio()

    try:
        try:
            get_name = lambda object: object.object_name
            names = map(get_name, client.list_objects_v2(bucket_name, '', recursive=True))
            for err in client.remove_objects(bucket_name, names):
                print("Deletion Error: {}".format(err))
        except ResponseError as err:
            print(err)

        client.remove_bucket(bucket_name)
    except Exception as e:
        print(e)

    try:
         client.make_bucket(bucket_name, location="us-east-1")
    except BucketAlreadyOwnedByYou as err:
         pass
    except BucketAlreadyExists as err:
         pass
    except ResponseError as err:
         raise
    else:
        # Put an object 'pumaserver_debug.log' with contents from 'pumaserver_debug.log'.
        try:
             print(client.put_object(bucket_name, 'result', io.BytesIO(result_json.encode()), len(result_json)))
             print(client.put_object(bucket_name, 'inputs', io.BytesIO(json.dumps(inputs).encode()), len(json.dumps(inputs))))
             print(client.put_object(bucket_name, 'meta', io.BytesIO(json.dumps(meta).encode()), len(json.dumps(meta))))
             print("stored")
        except ResponseError as err:
             print(err)

def get_record():
#inputs, results, bucket_name):
    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/query',
                   params=dict(query='''PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dda: <http://ddahub.io/ontology/analysis#>

SELECT * WHERE {
  ?sub ?x ?y . 
  ?sub rdfs:subClassOf dda:CalibrationWorkflow .
} 
LIMIT 10
'''),
        auth=requests.auth.HTTPBasicAuth("admin", open(os.path.join(os.environ.get('HOME'), '.jena-password')).read().strip())
    )


    print(r)

    print(r.text)



def create_record(inputs, results, bucket_name):

    entries="dda:{bucket_name} rdfs:subClassOf dda:CalibrationWorkflow . ".format(bucket_name=bucket_name)

    for k,v in inputs.items():
        entries+="\ndda:{k}Input rdfs:subClassOf dda:hasInput . ".format(k=k)
        entries+="\ndda:{bucket_name} dda:{k}Input \"{v}\" .".format(bucket_name=bucket_name, k=k, v=v)

    entries+="\ndda:{bucket_name} dda:usesCalibrationCandle db:Crab_pulsar .".format(bucket_name=bucket_name, k=k, v=v)

    print(entries)

    r=requests.post('http://fuseki.internal.odahub.io/dataanalysis/update',
                   data='''PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dda: <http://ddahub.io/ontology/analysis#>

    PREFIX dbprop: <http://dbpedia.org/property/>
    PREFIX db: <http://dbpedia.org/resource/>


    INSERT DATA
    { 
        %s
    }'''%entries,
        auth=requests.auth.HTTPBasicAuth("admin", open(os.path.join(os.environ.get('HOME'), '.jena-password')).read().strip())
    )


    print(r)
    print(r.text)

    


# find entity to run in rdf/jena

inputs=dict(
        t1="2018-08-10T13:35:15",
        t2="2018-09-19T13:35:15",
        nscw=5,
        chi2_limit=1.0,
        systematic_fraction=0.02,
)

import hashlib

hashdigest = hashlib.md5(json.dumps(inputs).encode()).hexdigest()

get_record()

bucket_name = "workflow-crab-"+hashdigest

print("loading", bucket_name)

try:
    r = load(bucket_name)
except:
    r = None

if r is not None:
    result_json, cwl_content = r
else:
    result_json, cwl_content = run(
        inputs
    ) # run with inputs in context
    # in reana?

    store(dict(name=bucket_name, notebook_url="https://github.com/volodymyrss/cc-crab/blob/master/crab.ipynb", cwl=cwl_content), inputs, result_json, bucket_name)

    create_record(inputs, result_json, bucket_name)



