# Licensed under a 3-clause BSD style license - see LICENSE.rst
import os
from distutils.version import LooseVersion
import astropy
try:
    from ...eso import Eso
    ESO_IMPORTED = True
except ImportError:
    ESO_IMPORTED = False
from astropy.tests.helper import pytest
from ...utils.testing_tools import MockResponse

SKIP_TESTS = not ESO_IMPORTED

DATA_DIR = os.path.join(os.path.dirname(__file__), 'data')


def data_path(filename):
    return os.path.join(DATA_DIR, filename)

DATA_FILES = {'GET': {'http://archive.eso.org/wdb/wdb/eso/amber/form':
                      'amber_form.html',
                      'http://archive.eso.org/wdb/wdb/adp/phase3_main/form':
                      'vvv_sgra_form.html',
                     },
              'POST': {'http://archive.eso.org/wdb/wdb/eso/amber/query':
                       'amber_sgra_query.tbl',
                       'http://archive.eso.org/wdb/wdb/adp/phase3_main/query':
                       'vvv_sgra_survey_response.tbl',
                      }
              }


def eso_request(request_type, url, **kwargs):
    with open(data_path(DATA_FILES[request_type][url]), 'rb') as f:
        response = MockResponse(content=f.read(), url=url)
    return response


# @pytest.fixture
# def patch_get(request):
#    mp = request.getfuncargvalue("monkeypatch")
#    mp.setattr(Eso, 'request', eso_request)
#    return mp

# This test should attempt to access the internet and therefore should fail
# (_activate_form always connects to the internet)
# @pytest.mark.xfail
@pytest.mark.skipif('SKIP_TESTS')
def test_SgrAstar(monkeypatch):
    # Local caching prevents a remote query here

    eso = Eso()

    # monkeypatch instructions from https://pytest.org/latest/monkeypatch.html
    monkeypatch.setattr(eso, '_request', eso_request)
    # set up local cache path to prevent remote query
    eso.cache_location = DATA_DIR

    # the failure should occur here
    result = eso.query_instrument('amber', target='Sgr A*')

    # test that max_results = 50
    assert len(result) == 50
    assert 'GC_IRS7' in result['Object']

bad_ascii_reader = LooseVersion(astropy.__version__) > LooseVersion('1.0.dev9885')

@pytest.mark.skipif('SKIP_TESTS or bad_ascii_reader')
def test_vvv(monkeypatch):

    result_s = eso.query_survey('VVV', coord1=266.41681662, coord2=-29.00782497)
    assert result_s is not None
    assert 'Object' in result_s.colnames
    assert 'b333' in result_s['Object']
