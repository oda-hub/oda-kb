import cwltool.factory
import requests
import pprint
import os
import json
import io
import ast
import click
import hashlib

from minio import Minio
from minio.error import (ResponseError, BucketAlreadyOwnedByYou,
             BucketAlreadyExists)

@click.group()
def cli():
    pass


def get_minio():
    return Minio('minio-internal.odahub.io',
              access_key='minio',
              secret_key=open(os.environ.get('HOME')+"/.minio").read().strip(),
              secure=False)

def restore(bucket, return_metadata = False):
    client = get_minio()
    try:
        data = client.get_object(bucket, 'data')
        meta = json.loads(client.get_object(bucket, 'meta').read())
    except Exception as e:
        raise

    if return_metadata:
        return meta, json.load(data)
    else:
        return json.load(data)

@cli.command("get")
@click.argument("bucket")
def _restore(bucket):
    m, d = restore(bucket, return_metadata=True)

    click.echo(pprint.pformat(m))

    for k,v in d.items():
        if not k.endswith("_content"):
            click.echo("{}: {}".format(k, pprint.pformat(v)))

@cli.command("rm")
@click.argument("bucket")
def _delete(bucket):
    client = get_minio()
    try:
        for o in client.list_objects(bucket):
            print("found object", o)
            client.remove_object(bucket, o.object_name)

        client.remove_bucket(bucket)
        print("removed bucket", bucket)
    except Exception as e:
        print("unable to remove bucket", bucket, e)


   

@cli.command("list")
def list_buckets():
    client = get_minio()
    for bucket in sorted(client.list_buckets(), key=lambda x:x.creation_date):
        meta = json.loads(client.get_object(bucket.name, 'meta').read())
        click.echo("{creation_date} {source_name:>10} {bucket_name:64}".format(
                    bucket_name=bucket.name, 
                    creation_date=bucket.creation_date, 
                    source_name=str(meta.get('kwargs', {}).get('source_name'))
                   ))


def store(data, meta=None, bucket_name = None):
    data_json = json.dumps(data)

        

    if bucket_name is None:
        if meta is None:
            bucket_name = form_bucket_name(data)
        else:
            bucket_name = form_bucket_name(meta)
    
    if meta is None:
        meta = {}

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
             print(client.put_object(bucket_name, 'data', io.BytesIO(data_json.encode()), len(data_json)))
             print(client.put_object(bucket_name, 'meta', io.BytesIO(json.dumps(meta).encode()), len(json.dumps(meta))))
             print("stored")
        except ResponseError as err:
             print(err)

    return bucket_name


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

if __name__ == "__main__":
    cli()

