import re
import base64
import logging
import pluggy
import html2text

import odakb

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def split_osa_version_arg(meta):
    _ = meta['kwargs']['osa_version'].split('--')
    meta['kwargs']['osa_version'] = _[0]
    if len(_) > 1:
        meta['kwargs']['osa_version_modifiers'] = _[1:]

    
def parse_html_pars(html):
    cell_python = html2text.HTML2Text().handle(html)    
    logger.debug("cell_python: %s", cell_python)

    pars = {}
    for k, v in [l.split("=", 1) for l in cell_python.split("\n") if len(l.split("=", 1)) == 2]:
        v_no_comment = v.split("#", 1)[0]
        pars[k.strip()] = v.strip("\"\' ")

    logger.debug("pars: %s", pars)

    return pars

def extract_params(bucket):
    from odakb.datalake import restore # else not initialized

    meta, data = restore(bucket, return_metadata=True)
    
    output_notebook_html = base64.b64decode(data['output_notebook_html_content']).decode()

    with open("f.html", "w") as f:
        f.write(output_notebook_html)

    r = re.search(r"""<div class=".*?celltag_parameters">.*?(<pre>.*?</pre>)</div>""", output_notebook_html, re.S | re.M)
    default_pars = parse_html_pars(r.group(1))    
    
    r = re.search(r"""<div class=".*?celltag_injected-parameters">.*?(<pre>.*?</pre>)</div>""", output_notebook_html, re.S | re.M)
    pars = parse_html_pars(r.group(1))    
    
    return {**default_pars, **pars}

@pluggy.HookimplMarker("odakb_reindex")
def index_bucket(bucket, meta, client):

    logger.info("indexing %s", bucket)
    logger.info("meta %s", meta)

    split_osa_version_arg(meta)

    args = {**extract_params(bucket), **meta['kwargs']}

    #data = client.get_object(bucket, 'data')    


    v = f'''    
            oda:bucket-{bucket} oda:evaluation_of <{meta['query']}>;
                                oda:bucket "{bucket}";'''

    # TODO: detect used dependent workflow!


    # for arg in ['osa_version', 'source_name', 'nscw']:
    for arg in args.keys():
        if arg in args:
            v += f'''
                                oda:arg_{arg} "{args[arg]}";'''
        
    print("to ingest:", v)


    ## 
    

    odakb.sparql.insert(v)