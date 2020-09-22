# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""

@author: Javier Duran
@contact: javier.duran@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 13 Aug. 2018


"""
import pytest
import os

from requests.models import Response
from astroquery.esa.integral import IntegralClass
from astroquery.esa.integral.tests.dummy_tap_handler import DummyIntegralTapHandler
from astroquery.utils.testing_tools import MockResponse
from astropy import coordinates
from unittest.mock import MagicMock
from astropy.table.table import Table
import shutil


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


def get_mockreturn(method, request, url, *args, **kwargs):
    file = 'm31.vot'
    response = data_path(file)
    print("response " + response)
    shutil.copy(response + '.test', response)
    return response


@pytest.fixture(autouse=True)
def isla_request(request):
    try:
        mp = request.getfixturevalue("monkeypatch")
    except AttributeError:  # pytest < 3
        mp = request.getfuncargvalue("monkeypatch")
    mp.setattr(IntegralClass, '_request', get_mockreturn)
    return mp


class TestESAIntegral():

    def get_dummy_tap_handler(self):
        parameterst = {'query': "select top 5 * from ila.scw",
                       'output_file': "test2.vot",
                       'output_format': "votable",
                       'verbose': False}
        dummyTapHandler = DummyIntegralTapHandler("launch_job", parameterst)
        return dummyTapHandler

    def test_search_metadata(self):
        parameters = {'piname': "Jourdain",
                      'verbose': True}
        isla = IntegralClass(self.get_dummy_tap_handler())
        isla.search_metadata(parameters['piname'],
                             parameters['verbose'])

    def test_data_download(self):
        parameters = {'revid': "0127",
                      'filename': "scw_1.tar",
                      'verbose': True}
        isla = IntegralClass(self.get_dummy_tap_handler())
        isla.data_download(parameters['revid'],
                           parameters['filename'],
                           parameters['verbose'])

    def test_query_tap(self):
        isla = IntegralClass(self.get_dummy_tap_handler())
        isla.query_tap(query="select top 5 * from ila.scw",
                       verbose=True)

    def test_get_position(self):
        parameters = {'targetname': "m31",
                      'verbose': True}
        isla = IntegralClass(self.get_dummy_tap_handler())
        isla.get_position(targetname=parameters['targetname'],
                          verbose=parameters['verbose'])

    def test_search_target(self):
        parameters = {'name': "m31",
                      'verbose': True}
        isla = IntegralClass(self.get_dummy_tap_handler())
        isla.search_target(name=parameters['name'],
                          verbose=parameters['verbose'])

if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unzip_test_files()
    pytest.main()
