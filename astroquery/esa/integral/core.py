# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""

======================
Integral Astroquery Module
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
import sys
import shutil
import os
import io
import time
import json

from . import conf
from astropy import log
from http.client import HTTPResponse

__all__ = ['Integral', 'IntegralClass']


class IntegralClass(BaseQuery):
    """
    Class to init ESA Integral Module and communicate with isla
    """

    TIMEOUT = conf.TIMEOUT

    def __init__(self, tap_handler=None):
        super(IntegralClass, self).__init__()

        if tap_handler is None:
            self._tap = TapPlus(url="https://ila.esac.esa.int/tap/tap",
                                data_context='data')
            self._data = "https://ila.esac.esa.int/tap/data"
        else:
            self._tap = tap_handler
            self._data = None

    def login(self, verbose=False):
        self._tap.login_gui(verbose)

    def logout(self, verbose=False):
        self._tap.logout(verbose)

    def search_target(self, name, filename=None, outputformat='votable',
                     verbose=False):
        """

        Parameters
        ----------
        name : string
            target name to be requested, mandatory
        filename : string
            file name to be used to store the metadata, optional, default None
        outputformat : string
            optional, default 'votable'
            output format of the query
        verbose : bool
            optional, default 'False'
            Flag to display information about the process

        Returns
        -------
        Table with the result of the query. It downloads metadata as a file.
        """
        return self.search_metadata(targetname=name,
                               outputformat=outputformat,
                               verbose=verbose)

    def search_metadata(self, targetname=None, piname=None,
               revno=None, srcname=None,
               obsid=None, starttime=None, endtime=None,
               asyncjob=True, filename=None,
               outputformat="votable", getquery=False,
               verbose=False):
        """
        Launches a synchronous or asynchronous job to query the isla tap
        using different fields as criteria to create and execute the
        associated query.

        Parameters
        ----------
        piname : str, optional
            P.I. name
        targetname : str, optional
            Target name
        revno : str, optional
            Revolution number
        srcname : str, optional
            Source name
        obsid : str, optional
            Observation id
        starttime : str, optional
            Start time. e.g. '2020-02-12 00:00:00.0'
        endtime : str, optional
            End time. e.g. '2020-07-08 00:00:00.0'
        async_job : bool, optional, default 'True'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        get_query : bool, optional, default 'False'
            flag to return the query associated to the criteria as the result
            of this function.
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """

        parameters = []
        if targetname is not None:
            parameters.append(self.__get_target_filter(targetname))
        if piname is not None:
            parameters.append("surname like '%{}%'".format(piname))
        if revno is not None:
            parameters.append("start_revno <= '{}' and end_revno >= '{}'".format(revno, revno))
        if srcname is not None:
            parameters.append("srcname like '%{}%'".format(srcname))
        if obsid is not None:
            parameters.append("obsid like '%{}%'".format(obsid))
        if starttime is not None and endtime is not None:
            parameters.append("starttime <= '{}'".format(starttime))
            parameters.append("endtime >= '{}'".format(endtime))
        elif starttime is None and endtime is not None:
            parameters.append("endtime == '{}'".format(endtime))
        elif endtime is None and starttime is not None:
            parameters.append("starttime == '{}'".format(starttime))

        query = "select * from ila.cons_observation"

        if parameters:
            query += " where ({})".format(" and ".join(parameters))

        query += " order by obsid"

        if verbose:
            log.info(query)

        table = self.query_tap(query=query, asyncjob=asyncjob,
                                   filename=filename,
                                   outputformat=outputformat,
                                   verbose=verbose)
        if getquery:
            return query
        return table

    def data_download(self, scwid=None,
                          obsid=None, revid=None,
                          propid=None, asyncjob=True,
                          filename=None,
                          outputformat="votable", verbose=False):
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
        asyncjob : bool, optional, default 'True'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        filename : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, 'download.tar' is used instead
        outputformat : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """

        parameters = {}
        if scwid is not None:
            parameters["scwid"] = scwid
        if obsid is not None:
            parameters["obsid"] = obsid
        if revid is not None:
            parameters["REVID"] = revid
        if propid is not None:
            parameters["PROPID"] = propid
        parameters["RETRIEVAL_TYPE"] = "SCW"

        response = None

        if self._data is not None:
            response = self._request('GET', self._data, save=True, cache=False,
                                 params=parameters)

        if verbose:
            log.info(self._data)
            log.info(parameters)

        if filename is None:
            filename = "isla_download_{}.tar".format(str(time.time()))

        if response is not None:
            shutil.move(response, filename)

    def query_tap(self, query, asyncjob=False, filename=None,
                      outputformat="votable", verbose=False):
        """Launches a synchronous or asynchronous job to query the isla tap

        Parameters
        ----------
        query : str, mandatory
            query (adql) to be executed
        asyncjob : bool, optional, default 'False'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        filename : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        outputformat : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """

        if asyncjob:
            job = self._tap.launch_job_async(query=query,
                                             output_file=filename,
                                             output_format=outputformat,
                                             verbose=verbose,
                                             dump_to_file=filename
                                             is not None)
        else:
            job = self._tap.launch_job(query=query, output_file=filename,
                                       output_format=outputformat,
                                       verbose=verbose,
                                       dump_to_file=filename is not None)
        table = job.get_results()
        return table

    def get_tables(self, onlynames=True, verbose=False):
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

        tables = self._tap.load_tables(only_names=onlynames,
                                       include_shared_tables=False,
                                       verbose=verbose)
        if onlynames is True:
            tablenames = []
            for t in tables:
                tablenames.append(t.name)
            return tablenames
        else:
            return tables

    def get_columns(self, tablename, onlynames=True, verbose=False):
        """Get the available columns for a table in isla TAP service

        Parameters
        ----------
        tablename : string, mandatory, default None
            table name of which, columns will be returned
        onlynames : bool, TAP+ only, optional, default 'False'
            True to load table names only
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A list of columns
        """

        tables = self._tap.load_tables(only_names=onlynames,
                                       include_shared_tables=False,
                                       verbose=verbose)
        columns = None
        for t in tables:
            if str(t.name) == str(tablename):
                columns = t.columns
                break

        if columns is None:
            raise ValueError("table name specified is not found in "
                             "Integral TAP service")

        if onlynames is True:
            columnnames = []
            for c in columns:
                columnnames.append(c.name)
            return columnnames
        else:
            return columns

    def get_position(self, targetname, verbose=False):
        p = {}
        if targetname is not None:
            url = "https://ila.esac.esa.int/tap/servlet/target-resolver?"\
                  "TARGET_NAME={}&RESOLVER_TYPE=ALL&FORMAT=json".format(targetname)
            response = self._request('GET', url)

            if "TARGET NOT FOUND" not in response.text:
                output = json.loads(response.text)
                ra = output['objects'][0]['raDegrees']
                dec = output['objects'][0]['decDegrees']
                p = {'ra': ra, "dec": dec}
            else:
                log.info(response.text)
                p = None

        if verbose:
            log.info(p)

        return p

    def __get_target_filter(self, targetname):
        filter = ""
        if targetname is not None:
            position = self.get_position(targetname)
            if position is not None:
                ra = position['ra']
                dec = position['dec']
                if ra is not None or dec is not None:
                    filter = "1=CONTAINS(POINT('ICRS',ra,dec),CIRCLE('ICRS',{},{},1))".format(ra, dec)
        return filter

    def __getCoordInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value,
                                                     commons.CoordClasses)):
            raise ValueError(str(msg) + ""
                             " must be either a string or astropy.coordinates")
        if isinstance(value, str):
            c = commons.parse_coordinates(value)
            return c
        else:
            return value

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


Integral = IntegralClass()
