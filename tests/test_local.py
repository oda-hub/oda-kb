import pytest

def test_directory_local():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d.keys())
    assert d
    
    evaluate("./code/odahub_io_test_testw")
    
    print(d.keys())
    assert d

def test_directory_local_file():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d.keys())
    assert d
    
    evaluate("./code/odahub_io_test_testw/test.ipynb")
    
    print(d.keys())
    assert d


def test_local():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d.keys())

    assert d


def test_local_parameterized():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw", t0=999)
    #d = evaluate("http://odahub.io/test/testw", ":t0 :value 999 .")

    print(d.keys())

    assert d


@pytest.mark.parametrize('require_version', [None, '5b5a1db', 'bad'])
def test_local_cwd(require_version):
    from odakb import evaluate

    from path import Path
    
    if require_version is None:
        d = evaluate("http://odahub.io/test/testw", t0=999)
    elif require_version == 'bad':        
        with pytest.raises(RuntimeError) as e:
            d = evaluate("http://odahub.io/test/testw", t0=999, _require_version=require_version)

    with Path("code/odahub_io_test_testw"):
        d = evaluate("http://odahub.io/test/testw")

    print(d.keys())

    assert d

def test_local_cwd_gitlab():
    from odakb import evaluate

    from path import Path

    with Path("code/odahub_io_test_testw"):
        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow.git")

    print(d.keys())

    assert d

def test_local_gitlab_mulitnb():
    from odakb import evaluate


    d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", nbname="test2")

    print(d.keys())

    assert d

def test_local_gitlab_mulitnb_fail():
    from odakb import evaluate

    try:
        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", _cached=False)
        print(d)
    except:
        pass
    else:
        raise Exception("this did not fail")


def test_local_gitlab_mulitnb_ssh():
    from odakb import evaluate


    d = evaluate("git@gitlab.astro.unige.ch:savchenk/oda-testworkflow-2nb.git", nbname="test2", _cached=False)

    print(d.keys())

    assert d

def test_git4ci():
    from odakb import evaluator as ev 
    assert ev.git4ci("git@gitlab.astro.unige.ch:integral/cc-workflows/cc-herx1.git") == "https://gitlab-ci-token:REDACTED@gitlab.astro.unige.ch/integral/cc-workflows/cc-herx1.git"

def test_local_gitlab_mulitnb_ssh_fail():
    from odakb import evaluate


    try:
        d = evaluate("git@gitlab.astro.unige.ch:savchenk/oda-testworkflow.git", induce_fail=1, _cached=False)
    except Exception as e:
        print(e)
    else:
        raise Exception("did not fail")


