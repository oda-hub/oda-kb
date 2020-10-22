import copy
import os
import re
import io
import json
import pprint
import time
import glob
import sys
import yaml
import rdflib # type: ignore
import pkg_resources
import requests
import typing
import importlib

import click
import logging

from os import getenv

#import keyring
#from keyrings.cryptfile.cryptfile import CryptFileKeyring
#kr = CryptFileKeyring()
#kr.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD") or None 
#keyring.set_keyring(kr)
#keyring.keyring_key = getenv("KEYRING_CRYPTFILE_PASSWORD", None) 

def placeholder(*a, **aa): # else lint gets confused by our tricks
    raise Exception("not overrided")
    return a,aa

update = placeholder
select = placeholder
insert = placeholder


logger = logging.getLogger("odakb.sparql")

def setup_logging(level=logging.INFO):
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    logger.setLevel(level)


# TODO: allow to produce local context, also offline

G = rdflib.Graph()


class LocalGraphClass:
    default_prefixes = [] # type: typing.List
    default_graphs = [] # type: typing.List

LocalGraph = LocalGraphClass()

def dump_loggers():
    for k,v in  logging.Logger.manager.loggerDict.items()  :
        print('+ [%s] {%s} ' % (str.ljust( k, 20)  , str(v.__class__)[8:-2]) ) 
        if not isinstance(v, logging.PlaceHolder):
            for h in v.handlers:
                print('     +++',str(h.__class__)[8:-2] )

def set_silent():
    setup_logging(logging.ERROR)

def set_debug():
    setup_logging(logging.DEBUG)
    

def parse_shortcuts(graph_serial):
    g = graph_serial.replace("="," an:equalTo ")
    if not g.strip().endswith("."):
        g += " ."

    return g

def load_graph(G, serial, shortcuts=False):
    if serial.startswith("https://"):
        G.load(serial)
    else:
        logger.info("will load: %s", serial)
        G.parse(
            data="\n".join(LocalGraph.default_prefixes) + (lambda x:x if not shortcuts else parse_shortcuts)(serial),
            format="turtle"
        )


def load_defaults(default_prefixes, default_graphs):

    for odakb_defaults in [
                os.path.join("/etc/odakb/defaults.yaml"),
                os.path.join(getenv("HOME"), ".odakb", "defaults.yaml"),
            ]:
        try:
            logger.info("oda defaults from %s", odakb_defaults)

            for p in yaml.safe_load(open(odakb_defaults))['prefixes']:
                if p not in default_prefixes:
                    logger.info("appending new prefix: %s", p)
                    default_prefixes.append(p)
        except Exception as e:
            logger.info("unable to load default prefixes from %s: %s", odakb_defaults, repr(e))
    
    try:
        odakb_defaults_http = "http://ontology.odahub.io/defaults/defaults.yaml"
        logger.info("oda defaults from %s", odakb_defaults_http)

        for p in yaml.safe_load(io.StringIO(requests.get(odakb_defaults_http).text))['prefixes']:
            if p not in default_prefixes:
                logger.info("appending new prefix: %s", p)
                default_prefixes.append(p)
    except Exception as e:
        logger.info("unable to load default prefixes: %s", repr(e))
        raise

    try:
        odakb_graphs = glob.glob(os.path.join(getenv("HOME"), ".odakb", "graphs.d","*"))
        logger.info("default graphs from: %s", odakb_graphs)
        for oda_graph_fn in odakb_graphs:
            default_graphs.append(open(oda_graph_fn).read())
    except Exception as e:
        logger.info("unable to load default graphs: %s", e)


def process_graph_loaders(G):
    for default_graph in LocalGraph.default_graphs:
        logger.info("loading default graph %s", default_graph)
        load_graph(G, default_graph)

    q = """
        SELECT ?loader ?rm ?location WHERE {
            ?loader a oda:graphLoader;
                    oda:runMethod ?rm;
                    oda:location ?location .
        }
        """
    r = query(q)
        
    for loader, rm, loc in G.query(q):
        logger.info(loader, rm, loc)
        if str(rm) == "http://odahub.io/ontology#pythonModule":
            logger.info("will load python module %s", loc)
            mn, fn = loc.split(".")
            m = importlib.import_module(mn)
            getattr(m, fn)(G)
        else:
            raise Exception("unable to exploit run method %s"%rm)


def init():
    load_defaults(LocalGraph.default_prefixes, LocalGraph.default_graphs)

query_stats = None


def unclick(f):
    """aliases function as _function without click's decorators"""
    if f.__name__.startswith('_'):
        setattr(sys.modules[f.__module__], f.__name__[1:], f)
    return f


@click.group()
@click.option("-d", "--debug", is_flag=True, default=False)
@click.option("-q", "--quiet", is_flag=True, default=False)
@click.option("-p", "--prefixes", default=None)
@click.option("-s", "--service", default=None)
def cli(debug=False, quiet=False, prefixes=None, service=None):
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

    LocalGraph.service = service


def reset_stats_collection():
    global query_stats
    query_stats=[]

def stop_stats_collection():
    global query_stats
    query_stats=None

def note_stats(**kwargs):
    global query_stats

    if query_stats is not None:
        query_stats.append(kwargs)

def report_stats():
    logger.info(query_stats)

    if query_stats is not None:
        summary=dict(
            n_queries=len(query_stats),
            spent_seconds=sum([s['spent_seconds'] for s in query_stats]),
            spent_longest_seconds=max([s['spent_seconds'] for s in query_stats]),
            query_size=sum([s['spent_seconds'] for s in query_stats]),
        )

        return summary

class SPARQLException(Exception):
    pass

def get_jena_password():
    tried = []

    for n, m in {
                'environ': lambda:os.environ['JENA_PASSWORD'].strip(),
                #'keyring': lambda:keyring.get_password("jena", "admin"),
            }.items():
        try:
            r = m()
            logger.info("good JENA password from %s", n)
            return r
        except Exception as e:
            logging.warning("failed %s %s %s", n, m, e)
            tried.append([n, m, e])

    raise RuntimeError("no good jena password, tried: "+repr(tried))

def compose_sparql(body, prefixes=None):
    _prefixes = copy.deepcopy(LocalGraph.default_prefixes)
    if prefixes is not None:
        _prefixes += prefixes

    return "\n".join(_prefixes)+"\n\n"+body

# curl http://fuseki.internal.odahub.io/dataanalysis --data query='PREFIX oda: <http://odahub.io/ontology#>  CONSTRUCT WHERE {?w a oda:test; ?b ?c  . ?x ?y ?w} ' | python -c 'import sys, rdflib; print(rdflib.Graph().parse(data=sys.stdin.read(), format="turtle").serialize(format="json-ld", indent=4, sort_keys=True).decode())' | jq '.[]'

@cli.command("construct")
@click.argument("data")
@click.option("-j", "--jsonld", is_flag=True, default=False)
def _construct(data, jsonld):
    r = construct(data, jsonld)

    if jsonld:
        print(json.dumps(r, indent=2, sort_keys=True))
    else:
        print(r)


def construct(data, jsonld):
    init()

    data = compose_sparql("CONSTRUCT WHERE {\n" + data + "\n}")

    r = execute_sparql(compose_sparql(data), "query", True, True)

    if jsonld:
        j = rdflib.Graph().parse(data=r, format="turtle").serialize(format="json-ld", indent=4, sort_keys=True).decode()
        return json.loads(j)
    else:
        return r


@cli.command("query")
@click.argument("data")
@click.option("-e", "--endpoint", default="query")
def _execute_sparql(data, endpoint="query", service=None):
    init()
    r = execute_sparql(compose_sparql(data), endpoint, invalid_raise=True, raw=False, service=service)

    for e in r['results']['bindings']:
        logger.info("entry: %s", e)

def execute_sparql(data, endpoint, invalid_raise, raw=False, service=None):
    logger.debug("data: %s", repr(data))
        

    t0=time.time()

    if service is None:
        oda_sparql_root = os.environ.get("ODA_SPARQL_ROOT", )
        if oda_sparql_root:
            logger.info("using sparql endpoing from \033[32mODA_SPARQL_ROOT\033[0m environment variable")
        else:
            oda_sparql_root = "https://sparql.odahub.io/dataanalysis"
            logger.info("using default sparql endpoint")

    else:
        oda_sparql_root = service

    logger.info("ODA Knowledge Base (SPARQL) root is \033[32m%s\033[0m", oda_sparql_root)

    if endpoint == "update":    
        auth=requests.auth.HTTPBasicAuth("admin", 
                                         get_jena_password())
        r=requests.post(oda_sparql_root+"/"+endpoint,
                        data=data,
                        auth=auth
                        )
    elif endpoint == "query":
        auth=None
        r=requests.post(oda_sparql_root+"/"+endpoint,
                       params=dict(query=data)
                    )
    else:
        auth=None
        r=requests.post(oda_sparql_root,
                       params=dict(query=data)
                    )

    note_stats(spent_seconds=time.time()-t0, query_size=len(data))
    
    logger.debug(r)
    logger.debug(r.text)
    logger.debug(report_stats())

    
    if invalid_raise:
        if r.status_code in [200, 201, 204]:
            logger.info("ODA KB responds %s", r.status_code)
        else:
            logger.error("SPARQL failed code %s", r.status_code)
            logger.debug("requested: %s", data)
            logger.debug("serice returns %s", r.text)
        
            if r.status_code == 403:
                m = f"Acceess To ODA KB denied at {oda_sparql_root}\n"
                m += f"please refer to https://github.com/volodymyrss/oda-kb.git for more info "
                logger.error(m)
                raise SPARQLException(m,
                                      r.status_code, r.text)
            else:
                raise SPARQLException(r.status_code, r.text)

    if raw:
        return r.text

    try:
        return r.json()
    except:
        return {'problem-decoding': r.text}

@cli.command("update")
@click.argument("query")
@unclick
def _update(query, invalid_raise=True):
    data = compose_sparql(query)

    return execute_sparql(data, 'update', invalid_raise=invalid_raise)

@cli.command("insert")
@click.argument("query")
#@click.pass_context
@unclick
def _insert(query=None):
    init()
    return update(query="INSERT DATA {\n" + query  + "\n}")

def create(entries):
    
    return update("INSERT DATA {\n" + ("\n".join(["%s %s %s ."%t3 for t3 in entries])) + "\n}")

def query(query, invalid_raise=True):
    data = compose_sparql(query)

    return execute_sparql(data, 'query', invalid_raise=invalid_raise)

def tuple_list_to_turtle(tl):
    rdf = "\n".join(LocalGraph.default_prefixes)
        
    rdf += "\n\n"

    for t in tl:
        if isinstance(t, str):
            s = t
            logger.debug("from string: %s", s)
        elif isinstance(t, list) or isinstance(t, tuple):
            s = " ".join(map(render_uri, t))

            logger.debug("from list: %s", s)
        else:
            raise RuntimeError()
        
        rdf += "\n"+s+" ."


    return rdf

@cli.command("select")
@click.argument("query")
@click.argument("form", required=False, default=None)
@click.option("-j", "--json", "tojson", is_flag=True)
@click.option("-d", "--jdict", "tojdict", is_flag=True)
@click.option("-r", "--rdf", "tordf", is_flag=True)
@click.option("-n", "--limit", "limit")
@unclick
def _select(query=None, form=None, todict=True, tojson=False, tordf=False, tojdict=False, limit=100, only="*"):
    init()

    if form is None:
        form = query

    data = compose_sparql(f"SELECT DISTINCT {only} WHERE {{ {query} }}" + (f" LIMIT {limit}" if limit is not None else ""))

    print("data:", data)

    r = execute_sparql(data, 'query', invalid_raise=True)

    try:
        entries = [ { k: v['value'] for k, v in _r.items() } for _r in r['results']['bindings'] ]
    except Exception as e:
        raise RuntimeError("problem interpreting SPARQL response: %s raw response %s", e, r)

    if tordf or tojson or tojdict:
        rdf = tuple_list_to_turtle([render_rdf(form, e) for e in entries])

    if tordf:
        print(rdf)
        return rdf

    if tojson or tojdict:
        g = rdflib.Graph().parse(data=rdf, format='turtle') 
        jsonld = g.serialize(format='json-ld', indent=4, sort_keys=True).decode()

    if tojson:
        print(jsonld)
        return jsonld
    
    if tojdict:
        prefix_dict = {}
        for pl in LocalGraph.default_prefixes:
            p, u = pl.split()[1:]
            prefix_dict[p] = u.strip("<>")

        def shorten_uri(u):
            for k, v in prefix_dict.items():
                if u.startswith(v):
                    return k[:-1]+":"+u[len(v):]
            return u

        def jsonld2dict(j):
            if isinstance(j, list):
                if all(['@id' in i for i in j]):
                    return {shorten_uri(i['@id']):jsonld2dict(i) for i in j}
                
                if all(['@value' in i for i in j]):
                    return list(sorted([ i['@value'] for i in j]))

            if isinstance(j, dict):
                return {shorten_uri(k): jsonld2dict(v) for k,v in j.items()}

            return j

        jdict = jsonld2dict(json.loads(jsonld))
        
        print(json.dumps(jdict, sort_keys=True, indent=4))

        return jdict

    for e in entries:
        logger.info("found fact: %s", e)

    if todict:
        return entries 
    else:
        return r

class ManyAnswers(Exception):
    pass

class NoAnswers(Exception):
    pass

@cli.command("select-one")
@click.argument("query")
@unclick
def _select_one(query=None):
    r = select(query)

    if len(r) > 1:
        raise ManyAnswers(r)
    
    if len(r) == 0:
        raise NoAnswers() 

    r = r[0]

    return r

class InvalidURI(Exception):
    pass

def render_uri(uri, entry=None):
    if entry is None:
        entry={}

    r = uri.strip()

    if uri.startswith("?"):
        r = entry[uri[1:]]

    if r.startswith("http://"):
        r = "<%s>"%r
        return r

    if r.startswith("<"):
        if not r.strip("<>").startswith("http://"):
            raise InvalidURI(f"only accept http uri, not this: {r}")
        return r
    
    for p in LocalGraph.default_prefixes:
        _, s, l = p.split()
        if r.startswith(s):
            return "<"+l.strip("<>")+r[len(s):]+">"

    if r == "a":
        return r

    return "\"%s\""%r.strip("\"")

def nuri(uri):
    return render_uri(uri)

def render_rdf(query, entry=None):
    if entry is None:
        entry = {}

    s, p, o = map(lambda u: render_uri(u, entry), query.split(None, 2))

    return "%s %s %s"%(s, p, o)

@cli.command("delete")
@click.argument("query")
@click.argument("fact", required=False)
@click.option("-a", '--all-entries', default=False, is_flag=True)
@click.option("-n", default=10)
@unclick
def _delete(query=None, fact=None, todict=True, all_entries=False, n=10):
    init()

    if not all_entries:
        data = compose_sparql("DELETE DATA {\n" + query + "\n}")
        logger.debug(data)

        r = execute_sparql(data, 'update', invalid_raise=True)
    else:
        entries = select(query, limit=n)

        logger.info("found entries to delete:\n")

        if fact is None:
            fact = query

        rdf_es = []
        for entry in entries:
            logger.info(entry)

            rdf = render_rdf(fact, entry)
            logger.info("rdf %s", rdf)

            rdf_es.append(rdf)

        if len(rdf_es)<=n:
            logger.info("deleting...")

            data = compose_sparql("DELETE DATA {\n" + " .\n".join(rdf_es) + "\n}")

            try:
                r = execute_sparql(data, 'update', invalid_raise=True)
            except Exception as e:
                logger.error("problem with request: %s", data)
                raise
        else:
            logger.warning("refusing to delete %i > %i entries"%(len(rdf_es), n))


@cli.command("reason")
@click.argument("query")
@click.argument("fact")
@click.option("-c/-nc",'--commit/--no-commit', default=False)
@unclick
def _reason(query, fact, commit=False):
    new_facts = []

    logger.info("reason from query %s to fact %s", query, fact)

    for r in select(query):
        logger.debug("\napplying %s to fact %s", r, fact)

        newfact = fact
        for k, v in r.items():
            patt=r"\?%s\b"%k
            val=nuri(v)
            logger.debug("substituting variable %s with %s in %s", patt, val, fact)
            newfact = re.sub(patt, val, newfact)

        new_facts.append(newfact)

        logger.info("new fact: %s", newfact)

        if commit:
            insert(newfact)

    if not commit:
        logger.warning("not commiting - no facts applied!")

    return new_facts

@cli.command()
def info():
    pass

@cli.command()
def version():
    click.echo(pkg_resources.get_distribution("oda-knowledge-base").version)

if __name__ == "__main__":
    cli()
