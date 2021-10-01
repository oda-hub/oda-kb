import logging
import pluggy

import odakb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def split_osa_version_arg(meta):
    _ = meta['kwargs']['osa_version'].split('--')
    meta['kwargs']['osa_version'] = _[0]
    if len(_) > 2:
        meta['kwargs']['osa_version_modifiers'] = _[1:]
    


@pluggy.HookimplMarker("odakb_reindex")
def index_bucket(bucket, meta, client):

    logger.info("indexing %s", bucket)
    logger.info("meta %s", meta)

    split_osa_version_arg(meta)

    args = meta['kwargs']

    #data = client.get_object(bucket, 'data')    


    v = f'''    
            oda:bucket-{bucket} oda:evaluation_of <{meta['query']}>;
                                oda:bucket "{bucket}";'''

    # TODO: detect used dependent workflow!

    for arg in ['osa_version', 'source_name', 'nscw']:
        if arg in args:
            v += f'''
                                oda:arg_{arg} "{args[arg]}";'''
        
    print("to ingest:", v)

    odakb.sparql.insert(v)