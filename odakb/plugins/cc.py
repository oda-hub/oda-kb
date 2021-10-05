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
        if k.strip().startswith("#"): continue
        pars[k.strip()] = v.strip("\"\' ")

    logger.debug("pars: %s", pars)

    return pars

def extract_params(data):

    output_notebook_html = base64.b64decode(data['output_notebook_html_content']).decode()

    with open("f.html", "w") as f:
        f.write(output_notebook_html)

    r = re.search(r"""<div class=".*?celltag_parameters">.*?(<pre>.*?</pre>)</div>""", output_notebook_html, re.S | re.M)
    default_pars = parse_html_pars(r.group(1))    
    
    r = re.search(r"""<div class=".*?celltag_injected-parameters">.*?(<pre>.*?</pre>)</div>""", output_notebook_html, re.S | re.M)
    pars = parse_html_pars(r.group(1))    
    
    return {**default_pars, **pars}

def interpret_summary(bucket_uri, data):
    print(data.keys())

    if 'summary' not in data:
        return ''
            
    try:
        isgri_times = data['summary']['isgri_times']
    except KeyError:
        raise RuntimeError        
    
    try:
        isgri_t1, isgri_t2 = map(float, isgri_times.split("--"))
    except:
        isgri_t1, isgri_t2 = isgri_times

    print(data['summary'])
    print(data['summary']['status'])

    print(data['summary']['isgri_times'])
    
    return f'''
    {bucket_uri} oda:out_status "{data['summary']['status']}" ;
                 oda:out_isgri_t1 "{isgri_t1}" ;
                 oda:out_isgri_t2 "{isgri_t2}" .
    '''


        

@pluggy.HookimplMarker("odakb_reindex")
def index_bucket(bucket_name, meta, client, creation_date_timestamp):

    logger.info("indexing %s", bucket_name)
    logger.info("meta %s", meta)

    split_osa_version_arg(meta)

    from odakb.datalake import restore # else not initialized
    meta, data = restore(bucket_name, return_metadata=True)


    args = {**extract_params(data), **meta['kwargs']}

    #data = client.get_object(bucket, 'data')    

    bucket_uri = f'oda:bucket-{bucket_name}'

    v = f'''    
            {bucket_uri}        oda:evaluation_of <{meta['query']}>;
                                oda:bucket "{bucket_name}";'''

    # TODO: detect used dependent workflow!


    # for arg in ['osa_version', 'source_name', 'nscw']:
    for arg in args.keys():
        if arg in args:
            v += f'''
                                oda:arg_{arg} "{args[arg]}";'''
    v += " .\n"
        
    v += interpret_summary(bucket_uri, data)    

    if creation_date_timestamp is not None:
        v += f'''
            {bucket_uri} oda:creation_date_timestamp {creation_date_timestamp} .
        '''

    print("to ingest:", v)
    odakb.sparql.insert(v)