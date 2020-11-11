import cwltool.factory # type: ignore
import requests
import pprint
import base64
import os
import sys
import json
import io
import ast
import click
import hashlib

import logging
 
def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger

logger = get_logger(__name__)

from minio import Minio # type: ignore
from minio.error import (ResponseError, BucketAlreadyOwnedByYou, # type: ignore
             BucketAlreadyExists, NoSuchBucket)


@click.group()
def cli():
    pass

def get_minio_secret():
    failures = {}
    for n, m in {
                "environ": lambda :os.environ['MINIO_KEY'].strip(),
                "homefile": lambda :open(os.environ.get('HOME')+"/.minio").read().strip(),
                }.items():
        try:
            return m()
        except Exception as e:
            failures[n] = e

    raise RuntimeError("unable to discover MINIO: "+repr(failures))

def get_minio():
    return Minio('minio-internal.odahub.io',
              access_key='minio',
              secret_key=get_minio_secret(),
              secure=False)

def exists(bucket):
    client = get_minio()
    return client.bucket_exists(bucket)

def restore(bucket, return_metadata = False, write_files=False):
    client = get_minio()
    try:
        data = client.get_object(bucket, 'data')
        meta = json.loads(client.get_object(bucket, 'meta').read())
    except Exception as e:
        raise

    decoded_data = json.load(data)

    if write_files:
        for k, v in decoded_data.items():
            print("found key", k)
            if k.endswith("_content"):
                print("found content, requested to store file", k, "size", len(v))
                open(k[:-len("_content")], "wb").write(base64.b64decode(v))

    if return_metadata:
        return meta, decoded_data
    else:
        return decoded_data

@cli.command("get")
@click.argument("bucket")
@click.option("-o","--output", default=None)
def _restore(bucket, output=None):
    m, d = restore(bucket, return_metadata=True)

    click.echo(pprint.pformat(m))

    for k,v in d.items():
        if not k.endswith("_content"):
            click.echo("{}: {}".format(k, pprint.pformat(v)))

    if output is not None:
        json.dump(d, open(output,"w"))
        click.echo("storing output to {}".format(output))

@cli.command("rm")
@click.argument("bucket")
def _delete(bucket):
    delete(bucket)

def delete(bucket):
    client = get_minio()
    try:
        for o in client.list_objects(bucket):
            logger.info("found object %s", o)
            client.remove_object(bucket, o.object_name)

        client.remove_bucket(bucket)
        logger.info("removed bucket %s", bucket)
    except Exception as e:
        logger.info("unable to remove bucket %s %s", bucket, e)


   

@cli.command("list")
def list_buckets():
    client = get_minio()

    for bucket in sorted(client.list_buckets(), key=lambda x:x.creation_date):
        try:
            meta = json.loads(client.get_object(bucket.name, 'meta').read())
            logger.info("{creation_date} {source_name:>10} {bucket_name:64}".format(
                        bucket_name=bucket.name, 
                        creation_date=bucket.creation_date, 
                        source_name=str(meta.get('kwargs', {}).get('source_name'))
                       ))
        except Exception as e:
            logger.warning("problematic bucket {}".format(bucket.name))

@cli.command("put")
@click.option("-b","--bucket", default=None)
@click.option("-m","--meta", default=None)
@click.option("-d","--data", default=None, help="ghrg")
def _put(bucket, meta, data):
    if data is None:
        logger.info("reading data from stdin")
        data = sys.stdin.read()
    elif data.startswith("@"):
        fn = data[1:]
        logger.info("reading data from %s",fn)
        data=open(fn).read()

    store(data, meta)

@cli.command("put-image")
@click.argument("fn")
def _put_image(fn):
    logger.info("reading data from %s",fn)
    data=open(fn, "rb").read()

    b = store({'image':base64.b64encode(data).decode()})
    print(b)
    

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
                logger.info("Deletion Error: {}".format(err))
        except ResponseError as err:
            logger.info(err)

        client.remove_bucket(bucket_name)
    except Exception as e:
        logger.info(e)

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
             logger.info(client.put_object(bucket_name, 'data', io.BytesIO(data_json.encode()), len(data_json)))
             logger.info(client.put_object(bucket_name, 'meta', io.BytesIO(json.dumps(meta).encode()), len(json.dumps(meta))))
             logger.info("stored")
        except ResponseError as err:
             logger.info(err)

    return bucket_name


def form_bucket_name(data):
    hashdigest = hashlib.md5(json.dumps(data).encode()).hexdigest()
    bucket_name = "b-"+hashdigest

    return bucket_name


if __name__ == "__main__":
    cli()

