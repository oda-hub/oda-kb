from datetime import datetime
import re
from typing import Tuple, Union
import cwltool.factory # type: ignore
import requests
import pprint
import base64
import os
import sys
import json
import io
import ast
import glob
import click
import hashlib

import odakb.sparql
import odakb.plugins.cc


import logging
 
def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger

logger = get_logger(__name__)

try:
    import getpass
except ImportError:
    print("problem importing getpass, portability?")

from minio import Minio # type: ignore

try:
    from minio.error import (InvalidResponseError, BucketAlreadyOwnedByYou, # type: ignore
                 BucketAlreadyExists, NoSuchBucket)
except:
    from minio.error import (ResponseError, BucketAlreadyOwnedByYou, # type: ignore
                 BucketAlreadyExists, NoSuchBucket)



@click.group()
def cli():
    pass

def get_minio_url():
    failures = {}
    for n, m in {
                "environment variable (MINIO_URL)": lambda :os.environ['MINIO_URL'].strip(),
                "default": lambda :"minio3.internal.odahub.io",
                }.items():
        try:
            r=m()
            logger.info("\033[32mdiscovered minio\033[0m URL with %s:%s", n, r)
            return r
        except Exception as e:
            failures[n] = e

    raise RuntimeError("unable to discover MINIO url "+repr(failures))

def get_minio_user():
    failures = {}
    for n, m in {
                "environment variable (MINIO_USER)": lambda :os.environ['MINIO_USER'].strip(),
                "current-user": lambda :getpass.getuser(),
                }.items():
        try:
            r=m()
            logger.debug("\033[32mdiscovered minio user\033[0m with %s:%s", n, r)
            return r
        except Exception as e:
            failures[n] = e

    raise RuntimeError("unable to discover MINIO user "+repr(failures))

def get_minio_secret():
    failures = {}
    for n, m in {
                "environment variable (MINIO_KEY)": lambda :os.environ['MINIO_KEY'].strip(),
                "dot file in home: ~/.minio-key": lambda :open(os.environ.get('HOME')+"/.minio-key").read().strip(),
                }.items():
        try:
            r=m()
            logger.info("\033[32mdiscovered minio access key\033[0m with %s", n)
            return r
        except Exception as e:
            failures[n] = e

    raise RuntimeError("unable to discover MINIO secret key: "+repr(failures))

def get_minio():
    # may also load fom ~/.mc/config.json
    return Minio(get_minio_url(),
              access_key=get_minio_user(),
              secret_key=get_minio_secret(),
              secure=(os.environ.get("MINIO_SECURE", "no") == "yes")) #FIX!

def exists(bucket):
    client = get_minio()
    return client.bucket_exists(bucket)

def restore(bucket: str, return_metadata = False, write_files=False) -> Union[Tuple, dict]:
    bucket_name, object_data_name, object_meta_name = full_name_to_bucket_object(bucket)

    client = get_minio()
    try:
        data = client.get_object(bucket_name, object_data_name)
        meta = json.loads(client.get_object(bucket_name, object_meta_name).read())
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
@click.option("-m","--metadata", default=None)
def _restore(bucket, output=None, metadata=None):
    m, d = restore(bucket, return_metadata=True)

    click.echo(pprint.pformat(m))

    for k,v in d.items():
        if not k.endswith("_content"):
            click.echo("{}: {}".format(k, pprint.pformat(v)))

    if output is not None:
        json.dump(d, open(output,"w"))
        click.echo("storing output to {}".format(output))
    
    if metadata is not None:
        json.dump(m, open(metadata,"w"))
        click.echo(f"storing metadata to {metadata}")

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


import pluggy

hookspec = pluggy.HookspecMarker("odakb_reindex")

class MySpec:
    @hookspec
    def index_bucket(bucket_name, meta, client, creation_date_timestamp):
        pass

pm = pluggy.PluginManager("odakb_reindex")
pm.add_hookspecs(MySpec)
#pm.load_setuptools_entrypoints("eggsample")
pm.register(odakb.plugins.cc)


@cli.command("ingest-local-cc-cache")
def ingest():    
    client = get_minio()

    for b in glob.glob("cc-data/*"):
        bucket_name = b.replace('cc-data/', '')
        store(bucket_name=bucket_name, 
              data=json.load(open(b + "/data")),
              meta=json.load(open(b + "/meta")))



@cli.command("reindex")
@click.option("--recent-days", type=float, default=None)
@click.option("--only-cached", is_flag=True)
@click.option("--max-entries", type=int, default=None)
@click.option("--select", default="-cc-")
@click.option("--single-bucket", default=None)
def reindex(only_cached, max_entries, select, single_bucket, recent_days):    
    client = get_minio()

    index_bucket = pm.hook.index_bucket

    if single_bucket is not None:
        meta = json.loads(client.get_object(single_bucket, 'meta').read())
        index_bucket(bucket_name=single_bucket, meta=meta, client=client, creation_date_timestamp=None)
        return
        

    if only_cached:
        for bucket_dir in glob.glob("cc-data/*"):
            bucket_name = os.path.basename(bucket_dir)
            logger.debug("%s", bucket_dir)

            index_bucket(bucket_name=bucket_name, 
                         meta=json.load(open(os.path.join(bucket_dir, "meta"))),
                         client=client,
                         creation_date_timestamp=None
                         )
        return

    buckets = list(sorted(client.list_buckets(), key=lambda x:x.creation_date))

    logger.info("total buckets: %s", len(buckets))

    n_parsed = 0
    n_failed = 0

    skipped_too_old = 0
    skipped_filter_name = 0

    for i, bucket in enumerate(buckets):        
        if i % int(len(buckets)/10) == 0:
            logger.info("parsed %i/%i buckets, %i too old, %i mismatch name, %i good %i failed", 
                            i, 
                            len(buckets), 
                            skipped_too_old, 
                            skipped_filter_name, 
                            n_parsed,
                            n_failed)

        logger.debug("trying name %s", bucket.name)
        
        bucket_age_days = (datetime.now().timestamp() - bucket.creation_date.timestamp()) / 24 / 3600

        if recent_days is not None:
            if bucket_age_days > recent_days:
                logger.debug("skipping for age %s %s", bucket_age_days, recent_days)
                skipped_too_old += 1
                continue                

        #if bucket.name.startswith('odahub-b'): continue        
        if not re.search(select, bucket.name):
            logger.debug("skipping for name %s", bucket.name)
            skipped_filter_name += 1
            continue
        
        logger.info("bucket %s age %f days", bucket.name, bucket_age_days)
        
        try:
            logger.info("{creation_date} {bucket_name:64} {bucket}".format(
                        bucket_name=bucket.name, 
                        creation_date=bucket.creation_date, 
                        bucket=bucket
                       ))
            meta = json.loads(client.get_object(bucket.name, 'meta').read())
            data = json.loads(client.get_object(bucket.name, 'data').read())
            logger.info("{creation_date} {bucket_name:64} {meta} {data}".format(
                        bucket_name=bucket.name, 
                        creation_date=bucket.creation_date, 
                        meta=meta,
                        data=data.keys()
                       ))

            if 'kwargs' in meta and 'osa_version' in meta['kwargs']:
                # split_osa_version_arg(meta)

                logger.info("\033[31m%s\033[0m", meta['kwargs']['osa_version'])

                for k in 'meta', 'data':
                    d = f"cc-data/{bucket.name}"
                    os.makedirs(d, exist_ok=True)
                    with open(f"{d}/{k}", "wb") as f:
                        f.write(client.get_object(bucket.name, k).read())

                try:
                    index_bucket(bucket_name=bucket.name,                              
                                meta=json.load(client.get_object(bucket.name, 'meta')), 
                                client=client,
                                creation_date_timestamp=bucket.creation_date.timestamp())
                    n_parsed += 1
                except RuntimeError as e:
                    n_failed += 1
                    continue

                if max_entries is not None and max_entries < n_parsed:
                    break

        except Exception as e:
            #raise
            logger.warning("problematic bucket {} : {}".format(bucket.name, e))



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
    print(put_image(fn))

def put_image(fn):
    logger.info("reading data from %s",fn)
    data=open(fn, "rb").read()

    b = store_nested({'image':base64.b64encode(data).decode()}, prefix="images/oda-board")
    return b
    

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
                logger.error("Deletion Error: {}".format(err))
        except ResponseError as err:
            logger.debug("error removing bucket", err)

        client.remove_bucket(bucket_name)
    except Exception as e:
         logger.error("error removing bucket %s: %s", bucket_name, e)

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
             logger.debug("storing data to bucket returns %s", client.put_object(bucket_name, 'data', io.BytesIO(data_json.encode()), len(data_json)))
             logger.debug("storing meta-data to bucket returns %s", client.put_object(bucket_name, 'meta', io.BytesIO(json.dumps(meta).encode()), len(json.dumps(meta))))
             logger.debug("stored")
        except ResponseError as err:
             logger.warning("error storing bucket %s: %s", bucket_name, err)

    return bucket_name


def full_name_to_bucket_object(full_name):
    full_name_segments = full_name.split("/")

    if len(full_name_segments) > 1:
        bucket_name = full_name_segments[0]
        object_data_name = os.path.join(*full_name_segments[1:], 'data')     
        object_meta_name = os.path.join(*full_name_segments[1:], 'meta')     
    else:
        bucket_name = full_name
        object_data_name = "data"
        object_meta_name = "meta"

    logger.info("bucket name: %s", bucket_name)
    logger.info("object data name: %s", object_data_name)
    logger.info("object meta name: %s", object_meta_name)

    return bucket_name, object_data_name, object_meta_name

def store_nested(data, meta=None, name = None, prefix=None):
    data_json = json.dumps(data)
        

    if name is None:
        if meta is None:
            name = form_data_name(data)
        else:
            name = form_data_name(meta)
    
    if meta is None:
        meta = {}

    if prefix is None:
        full_name = name.strip("/")
    else:
        full_name = os.path.join(prefix, name).strip("/")

    bucket_name, object_data_name, object_meta_name = full_name_to_bucket_object(full_name)

    client = get_minio()
    
    try:
         client.make_bucket(bucket_name, location="us-east-1")
    except BucketAlreadyOwnedByYou as err:
         pass
    except BucketAlreadyExists as err:
         pass
    except ResponseError as err:
         raise

    try:
            logger.debug("storing data to bucket returns %s", client.put_object(bucket_name, object_data_name, io.BytesIO(data_json.encode()), len(data_json)))
            logger.debug("storing meta-data to bucket returns %s", client.put_object(bucket_name, object_meta_name, io.BytesIO(json.dumps(meta).encode()), len(json.dumps(meta))))
            logger.debug("stored")
    except ResponseError as err:
            logger.warning("error storing bucket %s: %s", bucket_name, err)

    return full_name


def form_bucket_name(data):
    hashdigest = hashlib.md5(json.dumps(data).encode()).hexdigest()
    bucket_name = "b-" + hashdigest

    return bucket_name

def form_data_name(data):
    hashdigest = hashlib.md5(json.dumps(data).encode()).hexdigest()    
    return "b-" + hashdigest

if __name__ == "__main__":
    cli()

