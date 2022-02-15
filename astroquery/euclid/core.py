# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
===============
Euclid TAP plus
===============
@author: Juan Carlos Segovia (original code from Gaia) / John Hoar (Euclid adaptation)
@contact: juan.carlos.segovia@sciops.esa.int / jhoar@sciops.esa.int
European Space Astronomy Centre (ESAC)
European Space Agency (ESA)
Created on 30 jun. 2016
Euclid adaptation: 31 January 2022
"""
from requests import HTTPError

from astroquery.utils.tap import TapPlus
from astroquery.utils import commons
from astroquery import log
from astropy import units
from astropy.units import Quantity
import six
from astroquery.utils.tap import taputils
from . import conf

__all__ = ['Euclid', 'EuclidClass']

MAIN_TABLE = "public.kids_dr4"
MAIN_TABLE_RA = "raj2000"
MAIN_TABLE_DEC = "decj2000"


class EuclidClass(TapPlus):

    """
    Proxy class to default TapPlus object (pointing to the Euclid archive)
    """

    ROW_LIMIT = conf.ROW_LIMIT

    def __init__(self, tap_plus_conn_handler=None,
                 datalink_handler=None,
                 euclid_tap_server='https://eas.esac.esa.int/',
                 euclid_data_server='https://eas.esac.esa.int/',
                 tap_server_context="tap-server",
                 data_server_context="sas-dd",
                 verbose=False):
        super(EuclidClass, self).__init__(url=euclid_tap_server,
                                        server_context=tap_server_context,
                                        tap_context="tap",
                                        upload_context="Upload",
                                        table_edit_context="TableTool",
                                        data_context="data",
                                        datalink_context="datalink",
                                        connhandler=tap_plus_conn_handler,
                                        verbose=verbose)

    def load_tables(self, only_names=False, include_shared_tables=False,
                    verbose=False):
        """Loads all public tables
        TAP & TAP+
        Parameters
        ----------
        only_names : bool, TAP+ only, optional, default 'False'
            True to load table names only
        include_shared_tables : bool, TAP+, optional, default 'False'
            True to include shared tables
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A list of table objects
        """
        return self.__taphandler.load_tables(only_names,
                                          include_shared_tables,
                                          verbose)

    def load_table(self, table, verbose=False):
        """Loads the specified table
        TAP+ only
        Parameters
        ----------
        table : str, mandatory
            full qualified table name (i.e. schema name + table name)
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A table object
        """
        return self.__taphandler.load_table(table, verbose)

    def launch_job(self, query, name=None, output_file=None,
                        output_format="votable", verbose=False,
                        dump_to_file=False, upload_resource=None,
                        upload_table_name=None):
        """Launches a synchronous job
        TAP & TAP+
        Parameters
        ----------
        query : str, mandatory
            query to be executed
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if uploadResource is provided, default None
            resource temporary table name associated to the uploaded resource
        Returns
        -------
        A Job object
        """
        return self.__taphandler.launch_job(query,
                                              name=name,
                                              output_file=output_file,
                                              output_format=output_format,
                                              verbose=verbose,
                                              dump_to_file=dump_to_file,
                                              upload_resource=upload_resource,
                                              upload_table_name=upload_table_name)

    def launch_job_async(self, query, name=None, output_file=None,
                         output_format="votable", verbose=False,
                         dump_to_file=False, background=False,
                         upload_resource=None, upload_table_name=None):
        """Launches an asynchronous job
        TAP & TAP+
        Parameters
        ----------
        query : str, mandatory
            query to be executed
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        background : bool, optional, default 'False'
            when the job is executed in asynchronous mode, this flag specifies whether
            the execution will wait until results are available
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if uploadResource is provided, default None
            resource temporary table name associated to the uploaded resource
        Returns
        -------
        A Job object
        """
        return self.__taphandler.launch_job_async(query,
                                               name=name,
                                               output_file=output_file,
                                               output_format=output_format,
                                               verbose=verbose,
                                               dump_to_file=dump_to_file,
                                               background=background,
                                               upload_resource=upload_resource,
                                               upload_table_name=upload_table_name)

    def load_async_job(self, jobid=None, name=None, verbose=False):
        """Loads an asynchronous job
        TAP & TAP+
        Parameters
        ----------
        jobid : str, mandatory if no name is provided, default None
            job identifier
        name : str, mandatory if no jobid is provided, default None
            job name
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A Job object
        """
        return self.__taphandler.load_async_job(jobid, name, verbose)

    
    def search_async_jobs(self, jobfilter=None, verbose=False):
        """Searches for jobs applying the specified filter
        TAP+ only
        Parameters
        ----------
        jobfilter : JobFilter, optional, default None
            job filter
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A list of Job objects
        """
        return self.__taphandler.search_async_jobs(jobfilter, verbose)

    def list_async_jobs(self, verbose=False):
        """Returns all the asynchronous jobs
        TAP & TAP+
        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        A list of Job objects
        """
        return self.__taphandler.list_async_jobs(verbose)

    def __query_object(self, coordinate, radius=None, width=None, height=None,
                       async_job=False, verbose=False):
        """Launches a job
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are provided
            radius (deg)
        width : astropy.units, required if no 'radius' is provided
            box width
        height : astropy.units, required if no 'radius' is provided
            box height
        async_job : bool, optional, default 'False'
            executes the query (job) in asynchronous/synchronous mode (default
            synchronous)
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        The job results (astropy.table).
        """
        coord = self.__getCoordInput(coordinate, "coordinate")
        job = None
        if radius is not None:
            job = self.__cone_search(coord, radius,
                                     async_job=async_job, verbose=verbose)
        else:
            raHours, dec = commons.coord_to_radec(coord)
            ra = raHours * 15.0  # Converts to degrees
            widthQuantity = self.__getQuantityInput(width, "width")
            heightQuantity = self.__getQuantityInput(height, "height")
            widthDeg = widthQuantity.to(units.deg)
            heightDeg = heightQuantity.to(units.deg)
            query = "SELECT DISTANCE(POINT('ICRS',"+str(MAIN_TABLE_RA)+","\
                + str(MAIN_TABLE_DEC)+"), \
                POINT('ICRS',"+str(ra)+","+str(dec)+")) AS dist, * \
                FROM "+str(MAIN_TABLE)+" WHERE CONTAINS(\
                POINT('ICRS',"+str(MAIN_TABLE_RA)+","\
                + str(MAIN_TABLE_DEC)+"),\
                BOX('ICRS',"+str(ra)+","+str(dec)+", "+str(widthDeg.value)+", "\
                + str(heightDeg.value)+"))=1 \
                ORDER BY dist ASC"
            if async_job:
                job = self.__taphandler.launch_job_async(query, verbose=verbose)
            else:
                job = self.__taphandler.launch_job(query, verbose=verbose)
        return job.get_results()

    def query_object(self, coordinate, radius=None, width=None, height=None,
                     verbose=False):
        """Launches a job
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinates, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are provided
            radius (deg)
        width : astropy.units, required if no 'radius' is provided
            box width
        height : astropy.units, required if no 'radius' is provided
            box height
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        The job results (astropy.table).
        """
        return self.__query_object(coordinate,
                                 radius,
                                 width,
                                 height,
                                 async_job=False,
                                 verbose=verbose)

    def query_object_async(self, coordinate, radius=None, width=None,
                           height=None, verbose=False):
        """Launches a job (async)
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinates, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are provided
            radius
        width : astropy.units, required if no 'radius' is provided
            box width
        height : astropy.units, required if no 'radius' is provided
            box height
        async_job : bool, optional, default 'False'
            executes the query (job) in asynchronous/synchronous mode (default synchronous)
        verbose : bool, optional, default 'False'
            flag to display information about the process
        Returns
        -------
        The job results (astropy.table).
        """
        return self.__query_object(coordinate,
                                 radius,
                                 width,
                                 height,
                                 async_job=True,
                                 verbose=verbose)

    def __cone_search(self, coordinate, radius, table_name=MAIN_TABLE,
                      ra_column_name=MAIN_TABLE_RA,
                      dec_column_name=MAIN_TABLE_DEC,
                      async_job=False,
                      background=False,
                      output_file=None, output_format="votable", verbose=False,
                      dump_to_file=False,
                      columns=[]):
        """Cone search sorted by distance
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        async_job : bool, optional, default 'False'
            executes the job in asynchronous/synchronous mode (default
            synchronous)
        background : bool, optional, default 'False'
            when the job is executed in asynchronous mode, this flag specifies
            whether the execution will wait until results are available
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        Returns
        -------
        A Job object
        """
        coord = self.__getCoordInput(coordinate, "coordinate")
        raHours, dec = commons.coord_to_radec(coord)
        ra = raHours * 15.0  # Converts to degrees
        if radius is not None:
            radiusQuantity = self.__getQuantityInput(radius, "radius")
            radiusDeg = commons.radius_to_unit(radiusQuantity, unit='deg')

        if columns:
            columns = ','.join(map(str, columns))
        else:
            columns = "*"

        query = """
                SELECT
                  {row_limit}
                  {columns},
                  DISTANCE(
                    POINT('ICRS', {ra_column}, {dec_column}),
                    POINT('ICRS', {ra}, {dec})
                  ) AS dist
                FROM
                  {table_name}
                WHERE
                  1 = CONTAINS(
                    POINT('ICRS', {ra_column}, {dec_column}),
                    CIRCLE('ICRS', {ra}, {dec}, {radius})
                  )
                ORDER BY
                  dist ASC
                """.format(**{'ra_column': ra_column_name,
                              'row_limit': "TOP {0}".format(self.ROW_LIMIT) if self.ROW_LIMIT > 0 else "",
                              'dec_column': dec_column_name, 'columns': columns, 'ra': ra, 'dec': dec,
                              'radius': radiusDeg, 'table_name': table_name})
        if async_job:
            return self.__taphandler.launch_job_async(query=query,
                                                      output_file=output_file,
                                                      output_format=output_format,
                                                      verbose=verbose,
                                                      dump_to_file=dump_to_file,
                                                      background=background)
        else:
            return self.__taphandler.launch_job(query=query,
                                                output_file=output_file,
                                                output_format=output_format,
                                                verbose=verbose,
                                                dump_to_file=dump_to_file)

    def cone_search(self, coordinate, radius=None,
                    table_name=MAIN_TABLE,
                    ra_column_name=MAIN_TABLE_RA,
                    dec_column_name=MAIN_TABLE_DEC,
                    output_file=None,
                    output_format="votable",
                    verbose=False,
                    dump_to_file=False,
                    columns=[]):
        """Cone search sorted by distance (sync.)
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        Returns
        -------
        A Job object
        """
        return self.__cone_search(coordinate,
                                  radius=radius,
                                  table_name=table_name,
                                  ra_column_name=ra_column_name,
                                  dec_column_name=dec_column_name,
                                  async_job=False,
                                  background=False,
                                  output_file=output_file,
                                  output_format=output_format,
                                  verbose=verbose,
                                  dump_to_file=dump_to_file, columns=columns)

    def cone_search_async(self, coordinate, radius=None,
                          table_name=MAIN_TABLE,
                          ra_column_name=MAIN_TABLE_RA,
                          dec_column_name=MAIN_TABLE_DEC,
                          background=False,
                          output_file=None, output_format="votable",
                          verbose=False, dump_to_file=False, columns=[]):
        """Cone search sorted by distance (async)
        TAP & TAP+
        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        background : bool, optional, default 'False'
            when the job is executed in asynchronous mode, this flag specifies whether
            the execution will wait until results are available
        output_file : str, optional, default None
            file name where the results are saved if dumpToFile is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        Returns
        -------
        A Job object
        """
        return self.__cone_search(coordinate,
                                  radius=radius,
                                  table_name=table_name,
                                  ra_column_name=ra_column_name,
                                  dec_column_name=dec_column_name,
                                  async_job=True,
                                  background=background,
                                  output_file=output_file,
                                  output_format=output_format,
                                  verbose=verbose,
                                  dump_to_file=dump_to_file,
                                  columns=columns)

    def remove_jobs(self, jobs_list, verbose=False):
        """Removes the specified jobs
        TAP+
        Parameters
        ----------
        jobs_list : str, mandatory
            jobs identifiers to be removed
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__taphandler.remove_jobs(jobs_list)

    def save_results(self, job, verbose=False):
        """Saves job results
        TAP & TAP+
        Parameters
        ----------
        job : Job, mandatory
            job
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__taphandler.save_results(job, verbose)

    def login(self, user=None, password=None, credentials_file=None,
              verbose=False):
        """Performs a login.
        TAP+ only
        User and password can be used or a file that contains user name and password
        (2 lines: one for user name and the following one for the password)
        Parameters
        ----------
        user : str, mandatory if 'file' is not provided, default None
            login name
        password : str, mandatory if 'file' is not provided, default None
            user password
        credentials_file : str, mandatory if no 'user' & 'password' are provided
            file containing user and password in two lines
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        try:
            log.info("Login to Euclid TAP server")
            TapPlus.login(self, user=user, password=password,
                          credentials_file=credentials_file,
                          verbose=verbose)
        except HTTPError as err:
            log.error("Error logging in TAP server")
            return
        u = self._TapPlus__user
        p = self._TapPlus__pwd

        try:
            log.info("Login to Euclid data server")
            """
            TapPlus.login(self.__taphandler, user=u, password=p,
                          verbose=verbose)
            """
        except HTTPError as err:
            log.error("Error logging in data server")
            log.error("Logging out from TAP server")
            TapPlus.logout(self, verbose=verbose)

    def login_gui(self, verbose=False):
        """Performs a login using a GUI dialog

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        try:
            log.info("Login to Euclid TAP server")
            TapPlus.login_gui(self, verbose=verbose)
        except HTTPError as err:
            log.error("Error logging in TAP server")
            return
        u = self._TapPlus__user
        p = self._TapPlus__pwd
        try:
            log.info("Login to Euclid data server")
            TapPlus.login(self.__taphandler, user=u, password=p,
                          verbose=verbose)
        except HTTPError as err:
            log.error("Error logging in data server")
            log.error("Logging out from TAP server")
            TapPlus.logout(self, verbose=verbose)

    def logout(self, verbose=False):
        """Performs a logout
        TAP+ only
        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__taphandler.logout(verbose)

    def __checkQuantityInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value, units.Quantity)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")

    def __getQuantityInput(self, value, msg):
        if value is None:
            raise ValueError("Missing required argument: '"+str(msg)+"'")
        if not (isinstance(value, str) or isinstance(value, units.Quantity)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")
        if isinstance(value, str):
            q = Quantity(value)
            return q
        else:
            return value

    def __checkCoordInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value, commons.CoordClasses)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")

    def __getCoordInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value, commons.CoordClasses)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")
        if isinstance(value, str):
            c = commons.parse_coordinates(value)
            return c
        else:
            return value


Euclid = EuclidClass()