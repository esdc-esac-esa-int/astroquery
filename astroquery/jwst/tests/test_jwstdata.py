# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
===============
JWST DATA Tests
===============

@author: Raul Gutierrez-Sanchez
@contact: raul.gutierrez@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 05 nov. 2018


"""
import unittest
import os
import pytest

from astroquery.jwst.tests.DummyTapHandler import DummyTapHandler
from astroquery.jwst.tests.DummyDataHandler import DummyDataHandler

from astroquery.jwst.core import JwstClass


def data_path(filename):
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    return os.path.join(data_dir, filename)


class TestData(unittest.TestCase):

    def test_get_product(self):
        dummyTapHandler = DummyTapHandler()
        jwst = JwstClass(dummyTapHandler)
        # default parameters
        parameters = {}
        parameters['artifact_id'] = None
        with pytest.raises(ValueError) as err:
            jwst.get_product()
        assert "Missing required argument: 'artifact_id'" in err.value.args[0]
        # dummyDataHandler.check_call('get_product', parameters)
        # test with parameters
        dummyTapHandler.reset()
        parameters = {}
        params_dict = {}
        params_dict['RETRIEVAL_TYPE'] = 'PRODUCT'
        params_dict['DATA_RETRIEVAL_ORIGIN'] = 'ASTROQUERY'
        params_dict['ARTIFACTID'] = 'my_artifact_id'
        parameters['params_dict'] = params_dict
        parameters['output_file'] = 'my_artifact_id'
        parameters['verbose'] = False
        jwst.get_product(artifact_id='my_artifact_id')
        dummyTapHandler.check_call('load_data', parameters)


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
