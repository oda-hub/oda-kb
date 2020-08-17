
def test_slurm():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw")

    print(d.keys())

    assert d


def test_slurm_parameterized():
    from odakb import evaluate

    d = evaluate("http://odahub.io/test/testw", t0=999)
    #d = evaluate("http://odahub.io/test/testw", ":t0 :value 999 .")

    print(d.keys())

    assert d


def test_slurm_cwd():
    from odakb import evaluate

    from path import Path

    with Path("code/odahub.io.test.testw"):
        d = evaluate("http://odahub.io/test/testw")

    print(d.keys())

    assert d


