def test_yaml_numpy_representers():
    import yaml
    import numpy as np
    from odakb.evaluator import add_numpy_representers
    add_numpy_representers()


    d = yaml.safe_dump(np.int64(5))
    print(d)
    assert yaml.safe_load(d) == np.int64(5)


def test_local():
    from odakb import evaluate
    from odakb.evaluator import resolve_callable

    from odakb.evaluator import add_numpy_representers
    add_numpy_representers()

    func = "http://odahub.io/test/testw-pypi"

    print(resolve_callable(func))

    d = evaluate(func, [2,3])

    print(d)

    assert d == 5
    
    d = evaluate(func, [2,3])
    
    assert d == 5
    
    d = evaluate(func, [2,4])

    assert d == 6

#def test_local():
#    from odakb import evaluate
#
#    d = evaluate("http://odahub.io/test/testw")
#
#    print(d.keys())
#
#    assert d
#
#
#def test_local_parameterized():
#    from odakb import evaluate
#
#    d = evaluate("http://odahub.io/test/testw", t0=999)
#    #d = evaluate("http://odahub.io/test/testw", ":t0 :value 999 .")
#
#    print(d.keys())
#
#    assert d
#
#
#def test_local_cwd():
#    from odakb import evaluate
#
#    from path import Path
#
#    with Path("code/odahub.io.test.testw"):
#        d = evaluate("http://odahub.io/test/testw")
#
#    print(d.keys())
#
#    assert d
#
#def test_local_cwd_gitlab():
#    from odakb import evaluate
#
#    from path import Path
#
#    with Path("code/odahub.io.test.testw"):
#        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow.git")
#
#    print(d.keys())
#
#    assert d
#
#def test_local_gitlab_mulitnb():
#    from odakb import evaluate
#
#    from path import Path
#
#    d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", nbname="test2")
#
#    print(d.keys())
#
#    assert d
#
#def test_local_gitlab_mulitnb_fail():
#    from odakb import evaluate
#    from path import Path
#
#    try:
#        d = evaluate("https://gitlab.astro.unige.ch/savchenk/oda-testworkflow-2nb.git", _cached=False)
#        print(d)
#    except:
#        pass
#    else:
#        raise Exception("this did not fail")
#
#
#def test_local_gitlab_mulitnb_ssh():
#    from odakb import evaluate
#
#    from path import Path
#
#    d = evaluate("git@gitlab.astro.unige.ch:savchenk/oda-testworkflow-2nb.git", nbname="test2", _cached=False)
#
#    print(d.keys())
#
#    assert d
#
#def test_git4ci():
#    from odakb import evaluator as ev 
#    assert ev.git4ci("git@gitlab.astro.unige.ch:integral/cc-workflows/cc-herx1.git") == "https://gitlab-ci-token:REDACTED@gitlab.astro.unige.ch/integral/cc-workflows/cc-herx1.git"
#
#def test_local_gitlab_mulitnb_ssh_fail():
#    from odakb import evaluate
#
#    from path import Path
#
#    try:
#        d = evaluate("git@gitlab.astro.unige.ch:savchenk/oda-testworkflow.git", induce_fail=1, _cached=False)
#    except Exception as e:
#        print(e)
#    else:
#        raise Exception("did not fail")
#
#
