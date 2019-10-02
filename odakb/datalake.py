import cwltool.factory
import requests
import os
import json
import io
import ast
import hashlib

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
             BucketAlreadyExists)

def get_minio():
    return Minio('minio-internal.odahub.io',
              access_key='minio',
              secret_key=open(os.environ.get('HOME')+"/.minio").read().strip(),
              secure=False)

def load(bucket):
    client = get_minio()
    try:
        result = client.get_object(bucket, 'result')
        inputs = client.get_object(bucket, 'inputs')
        meta = json.loads(client.get_object(bucket, 'meta').read())
    except Exception as e:
        raise

    return result, meta['cwl']


def store(meta, data_json, bucket_name):
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


def form_bucket_name(data):
    hashdigest = hashlib.md5(json.dumps(data).encode()).hexdigest()
    bucket_name = "b-"+hashdigest

    return bucket_name


def self_test():
    print("loading", bucket_name)

    try:
        r = load(bucket_name)
    except:
        r = None

    store(
        dict(name=bucket_name, notebook_url="https://github.com/volodymyrss/cc-crab/blob/master/crab.ipynb", cwl=cwl_content), 
        inputs, 
        result_json, 
        bucket_name
    )

    #create_record(inputs, result_json, bucket_name)
