import pytest
import os

@pytest.mark.skipif(os.environ.get('JENA_PASSWORD') is None, reason="no writable jena")
def test_put():
    import odakb
    import odakb.datalake
    import odakb.sparql

    b = odakb.datalake.store(dict(test=1),dict(testdata=2))

    assert odakb.datalake.exists(b)

    print(odakb.datalake.restore(b))

#    fn="test-restore.json"
#    print(odakb.datalake.restore(b, output=fn))
#    print(json.load(open(fn)))

    c = odakb.sparql.create([('oda:a','oda:b','oda:c')])
    
    c = odakb.sparql.create([
            ('oda:a','oda:b','oda:c'),
            ('oda:a','oda:b','oda:c1'),
        ])

    odakb.sparql.report_stats()

@pytest.mark.skipif(os.environ.get('JENA_PASSWORD') is None, reason="no writable jena")
def test_fail():
    import odakb
    import odakb.datalake
    import odakb.sparql
    
    try:
        c = odakb.sparql.create([
                ('oda:a','bla:','c'),
                ('oda:a','oda:b','c1'),
            ])
    except odakb.sparql.SPARQLException as e:
        print("e", repr(e))
    else:
        raise Exception("did not raise")

    odakb.sparql.report_stats()

