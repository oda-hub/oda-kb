
def test_local():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d)

    assert d


