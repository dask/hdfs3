import pytest


pytest.importorskip('dask')


def test_dask_deprecation():
    with pytest.warns(UserWarning) as w:
        import hdfs3.dask  # noqa
    assert len(w) == 1
    assert 'DeprecationWarning' in str(w[0])
