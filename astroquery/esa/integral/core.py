# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""

======================
eHST Astroquery Module
======================


@author: Javier Duran
@contact: javier.duran@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 07 July 2020


"""
from astroquery.utils import commons
from astropy import units
from astropy.units import Quantity
from astroquery.utils.tap.core import TapPlus
from astroquery.utils.tap.model import modelutils
from astroquery.query import BaseQuery
from astropy.table import Table
from six import BytesIO
import shutil
import os
import time

from . import conf
from astropy import log

__all__ = ['Integral', 'IntegralClass']


class IntegralClass(BaseQuery):
    """
    Class to init ESA Integral Module and communicate with isla
    """

    data_url = conf.DATA_ACTION

    TIMEOUT = conf.TIMEOUT
    copying_string = "Copying file to {0}..."

    def __init__(self, tap_handler=None):
        super(IntegralClass, self).__init__()

        if tap_handler is None:
            self._tap = TapPlus(url="https://isladev.esac.esa.int/tap-dev/tap",
                                data_context='data')
        else:
            self._tap = tap_handler

    def login(self, verbose=False):
        self._tap.login_gui(verbose)

    def logout(self, verbose=False):
        self._tap.logout(verbose)

    def download_product(self, observation_id, calibration_level="RAW",
                         filename=None, verbose=False):
        """
        Download products from isla

        Parameters
        ----------
        observation_id : string
            id of the observation to be downloaded, mandatory
            The identifier of the observation we want to retrieve, regardless
            of whether it is simple or composite.
        calibration_level : string
            calibration level, optional, default 'RAW'
            The identifier of the data reduction/processing applied to the
            data. By default, the most scientifically relevant level will be
            chosen. RAW, CALIBRATED, PRODUCT or AUXILIARY
        filename : string
            file name to be used to store the artifact, optional, default
            None
            File name for the observation.
        verbose : bool
            optional, default 'False'
            flag to display information about the process

        Returns
        -------
        None. It downloads the observation indicated
        """

        params = {"OBSERVATION_ID": observation_id,
                  "CALIBRATION_LEVEL": calibration_level}

        if filename is None:
            filename = observation_id + ".tar"

        response = self._request('GET', self.data_url, save=True, cache=True,
                                 params=params)

        if verbose:
            log.info(self.data_url + "?OBSERVATION_ID=" + observation_id +
                     "&CALIBRATION_LEVEL=" + calibration_level)
            log.info(self.copying_string.format(filename))

        shutil.move(response, filename)

    def query_target(self, name, filename=None, output_format='votable',
                     verbose=False):
        """
        https://ila.esac.esa.int/tap/servlet/target-resolver?
        TARGET_NAME=m31&RESOLVER_TYPE=ALL&FORMAT=json
        https://ila.esac.esa.int/tap/tap/sync?LANG=ADQL&REQUEST=doQuery&FORMAT=json&QUERY=
        select TOP 1000 * from ila.cons_observation where 
        1=CONTAINS(POINT('ICRS',ra,dec),CIRCLE('ICRS',10.68470833,41.26875%2C1)) order by obsid
        It executes a query over isla and download the json with the results.

        Parameters
        ----------
        name : string
            target name to be requested, mandatory
        filename : string
            file name to be used to store the metadata, optional, default None
        output_format : string
            optional, default 'votable'
            output format of the query
        verbose : bool
            optional, default 'False'
            Flag to display information about the process

        Returns
        -------
        Table with the result of the query. It downloads metadata as a file.
        """

        params = {"RESOURCE_CLASS": "OBSERVATION",
                  "SELECTED_FIELDS": "OBSERVATION",
                  "QUERY": "(TARGET.TARGET_NAME=='" + name + "')",
                  "RETURN_TYPE": str(output_format)}
        response = self._request('GET', self.metadata_url, save=True,
                                 cache=True,
                                 params=params)

        if verbose:
            log.info(self.metadata_url + "?RESOURCE_CLASS=OBSERVATION&"
                     "SELECTED_FIELDS=OBSERVATION&QUERY=(TARGET.TARGET_NAME"
                     "=='" + name + "')&RETURN_TYPE=" + str(output_format))
            log.info(self.copying_string.format(filename))
        if filename is None:
            filename = "target.xml"

        shutil.move(response, filename)

        return modelutils.read_results_table_from_file(filename,
                                                       str(output_format))

    def query_tap(self, query, async_job=False, output_file=None,
                      output_format="votable", verbose=False):
        """Launches a synchronous or asynchronous job to query the isla tap

        Parameters
        ----------
        query : str, mandatory
            query (adql) to be executed
        async_job : bool, optional, default 'False'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """
                
        if async_job:
            job = self._tap.launch_job_async(query=query,
                                             output_file=output_file,
                                             output_format=output_format,
                                             verbose=verbose,
                                             dump_to_file=output_file
                                             is not None)
        else:
            job = self._tap.launch_job(query=query, output_file=output_file,
                                       output_format=output_format,
                                       verbose=verbose,
                                       dump_to_file=output_file is not None)
        table = job.get_results()
        return table

    def data_download(self, scwid=None,
                          obsid=None, revid=None,
                          propid=None, async_job=True,
                          filename=None,
                          output_format="votable", verbose=False):
        """
        Launches a synchronous or asynchronous job to query the isla tap
        using scw_id, obs_id, rev_id and prop_id as criteria to create 
        and execute the associated query.

        Parameters
        ----------
        scwid : str, optional
        obsid : str, optional
        revid : str, optional
        propid : str, optional
        async_job : bool, optional, default 'True'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        filename : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, 'download.tar' is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """

        parameters = {}
        if scwid is not None:
            parameters["scwid"]=scwid
        if obsid is not None:
            parameters["obsid"]=obsid
        if revid is not None:
            parameters["REVID"]=revid
        if propid is not None:
            parameters["PROPID"]=propid
        parameters["RETRIEVAL_TYPE"]="SCW"

        response = self._request('GET', self.data_url, save=True, cache=True,
                                 params=parameters)

        if verbose:
            log.info(self.data_url)
            log.info(parameters)

        if filename is None:
            filename = "isla_download_{}.tar".format(str(time.time()))

        shutil.move(response, filename)

    def __get_calibration_level(self, calibration_level):
        condition = ""
        if(calibration_level is not None):
            if isinstance(calibration_level, str):
                condition = calibration_level
            elif isinstance(calibration_level, int):
                if calibration_level < 4:
                    condition = self.calibration_levels[calibration_level]
                else:
                    raise KeyError("Calibration level must be between 0 and 3")
            else:
                raise KeyError("Calibration level must be either "
                               "a string or an integer")
        return condition

    def __check_list_strings(self, list):
        if list is None:
            return False
        if list and all(isinstance(elem, str) for elem in list):
            return True
        else:
            raise ValueError("One of the lists is empty or there are "
                             "elements that are not strings")

    def get_tables(self, only_names=True, verbose=False):
        """Get the available table in isla TAP service

        Parameters
        ----------
        only_names : bool, TAP+ only, optional, default 'False'
            True to load table names only
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A list of tables
        """

        tables = self._tap.load_tables(only_names=only_names,
                                       include_shared_tables=False,
                                       verbose=verbose)
        if only_names is True:
            table_names = []
            for t in tables:
                table_names.append(t.name)
            return table_names
        else:
            return tables

    def get_columns(self, table_name, only_names=True, verbose=False):
        """Get the available columns for a table in isla TAP service

        Parameters
        ----------
        table_name : string, mandatory, default None
            table name of which, columns will be returned
        only_names : bool, TAP+ only, optional, default 'False'
            True to load table names only
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A list of columns
        """

        tables = self._tap.load_tables(only_names=False,
                                       include_shared_tables=False,
                                       verbose=verbose)
        columns = None
        for t in tables:
            if str(t.name) == str(table_name):
                columns = t.columns
                break

        if columns is None:
            raise ValueError("table name specified is not found in "
                             "EHST TAP service")

        if only_names is True:
            column_names = []
            for c in columns:
                column_names.append(c.name)
            return column_names
        else:
            return columns

    def _getCoordInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value,
                                                     commons.CoordClasses)):
            raise ValueError(str(msg) + ""
                             " must be either a string or astropy.coordinates")
        if isinstance(value, str):
            c = commons.parse_coordinates(value)
            return c
        else:
            return value


Integral = IntegralClass()
