import pytest
import re
import os
    

def common():
    import odakb
    import odakb.sparql
    odakb.sparql.init()
    odakb.sparql.set_debug()


def test_get():
    import odakb
    import odakb.sparql
    common()

    c = odakb.sparql.query('SELECT * WHERE { ?a oda:source ?c }')


def test_select():
    import odakb
    import odakb.sparql
    common()

    c = odakb.sparql.select('?a a oda:workflow')

    assert isinstance(c, list)

    assert all([isinstance(i, dict) for i in c])

def test_select_json():
    import odakb
    import odakb.sparql
    common()

    c = odakb.sparql.select('?a a oda:workflow', tojson=True)

    assert isinstance(c, str)

    import json
    json.loads(c)

def test_select_rdf():
    import odakb
    import odakb.sparql
    common()

    c = odakb.sparql.select('?a a oda:workflow', tordf=True)

    assert isinstance(c, str)

    import rdflib
    rdflib.Graph().parse(data=c, format="turtle")

def test_select_one():
    import odakb
    import odakb.sparql
    common()


    c = odakb.sparql.select_one('?w a oda:workflow; oda:describes oda:v0332')

    assert isinstance(c, dict)

    print(c)


@pytest.mark.skip
def test_loaders():
    import rdflib
    import odakb
    import odakb.sparql

    G = rdflib.Graph()

    odakb.sparql.process_graph_loaders(G)

@pytest.mark.skipif(os.environ.get('JENA_PASSWORD') is None, reason="no writable jena")
def test_reason():
    import odakb
    import odakb.sparql
    common()

    odakb.sparql.insert('oda:tmp_obj oda:tmp_requires oda:tmp_req1')
    odakb.sparql.insert('oda:tmp_req1 oda:tmp_requires oda:tmp_req2')

    nf = odakb.sparql.reason('?x oda:tmp_requires ?y . ?y oda:tmp_requires ?z', 
                        '?x oda:tmp_requires ?z')

    assert len(nf) == 1

    print(nf[0])

    n = lambda x:re.sub("<<", "<",  
                 re.sub(">>", ">",  
                 re.sub("^", "<",  
                 re.sub(" ", "> <",  
                 re.sub("$", ">",  
                 x.replace("oda:", "http://odahub.io/ontology#"))))))

    assert n(nf[0]) == n("oda:tmp_obj oda:tmp_requires oda:tmp_req2")

def test_render_rdf():
    from odakb.sparql import render_rdf
    common()

    assert render_rdf('oda:x a "x"', {}) == '<http://odahub.io/ontology#x> a "x"'
    assert render_rdf('oda:x a "x x x"', {}) == '<http://odahub.io/ontology#x> a "x x x"'

def test_nuri():
    from odakb.sparql import nuri, InvalidURI

    assert nuri("<http://odahub.io/ontology#bla>") == "<http://odahub.io/ontology#bla>"
    assert nuri("http://odahub.io/ontology#bla") == "<http://odahub.io/ontology#bla>"

    assert nuri("oda:bla") == "<http://odahub.io/ontology#bla>"
    assert nuri("odabla") == "\"odabla\""
    assert nuri("\"odabla\"") == "\"odabla\""
    
    assert nuri("a") == "a"

    try:
        nuri("<odabla>")
    except InvalidURI as e:
        print(e)
    else:
        raise Exception("did not raise!")

