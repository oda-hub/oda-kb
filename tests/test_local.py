
def test_local():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d)

    assert d


def test_local_parameterized():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw", t0=999)
    #d = evaluate("http://odahub.io/test/testw", ":t0 :value 999 .")

    print(d)

    assert d


def test_local_cwd():
    from odakb import evaluate

    from path import Path

    with Path("code/odahub.io.test.testw"):
        d = evaluate("http://odahub.io/test/testw")

    print(d)

    assert d

def test_local_cwd_gitlab():
    from odakb import evaluate

    from path import Path

    with Path("code/odahub.io.test.testw"):
        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow.git")

    print(d)

    assert d

def test_local_gitlab_mulitnb():
    from odakb import evaluate

    from path import Path

    d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", nbname="test2")

    print(d)

    assert d

def test_local_gitlab_mulitnb_fail():
    from odakb import evaluate

    from path import Path

    try:
        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", _cached=False)
        print(d)
    except:
        pass
    else:
        raise Exception("this did not fail")


def test_local_gitlab_mulitnb_ssh():
    from odakb import evaluate

    from path import Path

    d = evaluate("git@gitlab.astro.unige.ch:savchenk/oda-testworkflow-2nb.git", nbname="test2", _cached=False)

    print(d)

    assert d
