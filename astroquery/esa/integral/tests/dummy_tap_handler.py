# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""

@author: Javier Duran
@contact: javier.duran@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 30 Aug. 2018


"""
from astroquery.utils.tap.model.taptable import TapTableMeta
from astroquery.utils.tap.model.job import Job


class DummyIntegralTapHandler(object):

    def __init__(self, method, parameters):
        self.__invokedMethod = method
        self._parameters = parameters

    def launch_job(self, query, name=None, output_file=None,
                   output_format="votable", verbose=False, dump_to_file=False,
                   upload_resource=None, upload_table_name=None):
        self.__invokedMethod = 'launch_job'
        self._parameters['query'] = query
        self._parameters['name'] = name
        self._parameters['output_file'] = output_file
        self._parameters['output_format'] = output_format
        self._parameters['verbose'] = verbose
        self._parameters['dump_to_file'] = dump_to_file
        self._parameters['upload_resource'] = upload_resource
        self._parameters['upload_table_name'] = upload_table_name
        return Job(False)

    def launch_job_async(self, query, name=None, output_file=None,
                         output_format="votable", verbose=False,
                         dump_to_file=False, upload_resource=None,
                         upload_table_name=None):
        self.__invokedMethod = 'launch_job'
        self._parameters['query'] = query
        self._parameters['name'] = name
        self._parameters['output_file'] = output_file
        self._parameters['output_format'] = output_format
        self._parameters['verbose'] = verbose
        self._parameters['dump_to_file'] = dump_to_file
        self._parameters['upload_resource'] = upload_resource
        self._parameters['upload_table_name'] = upload_table_name
        return Job(False)
