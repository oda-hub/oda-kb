import sys
import time
import json
import logging
import click

from . import datalake, sparql

logger = logging.getLogger("oda.kb.sparql")

def setup_logging(level=logging.INFO):
    #handler = logging.StreamHandler(sys.stdout)
    #handler.setLevel(level)
    #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    #handler.setFormatter(formatter)
    #logger.addHandler(handler)
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    logger.setLevel(level)


@click.group()
@click.option("-d", "--debug", is_flag=True, default=False)
@click.option("-q", "--quiet", is_flag=True, default=False)
def cli(debug=False, quiet=False):
    if debug and quiet:
        raise Exception("can not be quiet and debug!")

    if debug:
        setup_logging(logging.DEBUG)
        logger.error('test erro')
        logger.info('test erro')
    elif quiet:
        setup_logging(logging.ERROR)
    else:
        setup_logging()

@cli.command("upload-image")
@click.argument("fn")
@click.option("-t", "--tag", multiple=True)
@click.option("-a", "--annotate", multiple=True)
def _upload_image(fn, tag=None, annotate=None):
    upload_image(fn, tag, annotate)

def upload_image(fn, tag=None, annotate=None):
    bucket = datalake.put_image(fn)

    url = f"http://in.internal.odahub.io/dataapi/evaluate?public&target={bucket}&jpath=.image&png"
    logger.info(f"stored as {url}")

    q = f'<{url}> a oda:image .'
    
    if tag is None:
        logging.debug("\033[31mno tag!\033[0m")
    else:
        for _tag in tag:
            q += f'\n<{url}> oda:tag "{_tag}" .'
            logging.debug("\033[31mquery: %s\033[0m", q)

    if annotate is not None:
        for _a in annotate:
            q += f'<{url}> {_a}'

    logger.info(q)    

    sparql.insert(q)


@cli.command("list-images")
@click.option("-t", "--tag", multiple=True)
def _list_images(tag=None):
    r = list_images(tag)
    print(json.dumps(r, sort_keys=True, indent=4))

def list_images(tag=None):
    return sparql.select("?url a oda:image; oda:tag ?tag; ?p ?o .", "?url ?p ?o", tojdict=True)

@cli.command()
def render_index():
    images = list_images()

    from jinja2 import Environment, FileSystemLoader
    template_dir = 'odakb/templates'
    env = Environment(loader=FileSystemLoader(template_dir))

    html = env.get_template("images.html").render(cards=[
            {
                'url': k, 
                'title': '',
                'tags': v['oda:tag'],
                'updated_isot': v.get('oda:updated_isot', '?'),
                **v,
                } for k, v in images.items()
        ])

    with open("index.html", "w") as f:
        f.write(html)