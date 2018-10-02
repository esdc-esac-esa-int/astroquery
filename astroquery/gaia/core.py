# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
Gaia TAP plus
=============

@author: Juan Carlos Segovia
@contact: juan.carlos.segovia@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 30 jun. 2016


"""

from astroquery.utils.tap import TapPlus
from astroquery.utils import commons
from astropy import units
from astropy.units import Quantity
from astropy import config as _config
import six


class Conf(_config.ConfigNamespace):
    """
    Configuration parameters for `astroquery.gaia`.
    """

    MAIN_GAIA_TABLE = _config.ConfigItem("gaiadr2.gaia_source",
                                         "GAIA source data table")
    MAIN_GAIA_TABLE_RA = _config.ConfigItem("ra",
                                            "Name of RA parameter in table")
    MAIN_GAIA_TABLE_DEC = _config.ConfigItem("dec",
                                             "Name of Dec parameter in table")


conf = Conf()


class GaiaClass(object):

    """
    Proxy class to default TapPlus object (pointing to Gaia Archive)
    """
    MAIN_GAIA_TABLE = conf.MAIN_GAIA_TABLE
    MAIN_GAIA_TABLE_RA = conf.MAIN_GAIA_TABLE_RA
    MAIN_GAIA_TABLE_DEC = conf.MAIN_GAIA_TABLE_DEC

    def __init__(self, tap_plus_handler=None, datalink_handler=None):
        if tap_plus_handler is None:
            self.__gaiatap = TapPlus(url="http://gea.esac.esa.int/",
                                     server_context="tap-server",
                                     tap_context="tap",
                                     upload_context="Upload",
                                     table_edit_context="TableTool",
                                     data_context="data",
                                     datalink_context="datalink",
                                     share_context="share",
                                     users_context="users")
        else:
            self.__gaiatap = tap_plus_handler
        if datalink_handler is None:
            self.__gaiadata = TapPlus(url="http://geadata.esac.esa.int/",
                                      server_context="data-server",
                                      tap_context="tap",
                                      upload_context="Upload",
                                      table_edit_context="TableTool",
                                      data_context="data",
                                      datalink_context="datalink",
                                      share_context="share",
                                      users_context="users")
        else:
            self.__gaiadata = datalink_handler

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
        return self.__gaiatap.load_tables(only_names,
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
        return self.__gaiatap.load_table(table, verbose)

    def load_data(self, ids, retrieval_type="epoch_photometry",
                  valid_data=True, band=None, format="VOTABLE", verbose=False):
        """Loads the specified table
        TAP+ only

        Parameters
        ----------
        ids : str list, mandatory
            list of identifiers
        retrieval_type : str, optional, default 'epoch_photometry'
            retrieval type identifier
        valid_data : bool, optional, default True
            By default, the epoch photometry service returns only valid data,
            that is, all data rows where flux is not null and
            rejected_by_photometry flag is not true. In order to retrieve
            all data associated to a given source without this filter,
            this request parameter should be included (valid_data=False)
        band : str, optional, default None, valid values: G, BP, RP
            By default, the epoch photometry service returns all the
            available photometry bands for the requested source.
            This parameter allows to filter the output lightcurve by its band.
        format : str, optional, default 'votable'
            loading format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """
        if retrieval_type is None:
            raise ValueError("Missing mandatory argument 'retrieval_type'")
        if ids is None:
            raise ValueError("Missing mandatory argument 'ids'")
        params_dict = {}
        if valid_data:
            params_dict['VALID_DATA'] = "true"
        else:
            params_dict['VALID_DATA'] = "false"
        if band is not None:
            if band != 'G' and band != 'BP' and band != 'RP':
                raise ValueError("Invalid band value '%s' (Valid values: " +
                                 "'G', 'BP' and 'RP)" % band)
            else:
                params_dict['BAND'] = band
        if isinstance(ids, six.string_types):
            ids_arg = ids
        else:
            if isinstance(ids, int):
                ids_arg = str(ids)
            else:
                ids_arg = ','.join(str(item) for item in ids)
        params_dict['ID'] = ids_arg
        params_dict['FORMAT'] = str(format)
        params_dict['RETRIEVAL_TYPE'] = str(retrieval_type)
        return self.__gaiadata.load_data(params_dict=params_dict,
                                         verbose=verbose)

    def get_datalinks(self, ids, verbose=False):
        """Gets datalinks associated to the provided identifiers
        TAP+ only

        Parameters
        ----------
        ids : str list, mandatory
            list of identifiers
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """
        return self.__gaiadata.get_datalinks(ids=ids, verbose=verbose)

    def launch_job(self, query, name=None, output_file=None,
                   output_format="votable", verbose=False, dump_to_file=False,
                   upload_resource=None, upload_table_name=None):
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
        upload_table_name: str, required if uploadResource is provided, default
        None
            resource temporary table name associated to the uploaded resource

        Returns
        -------
        A Job object
        """
        return self.__gaiatap.launch_job(query, name=name,
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
            when the job is executed in asynchronous mode, this flag specifies
            whether
            the execution will wait until results are available
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if uploadResource is provided, default
        None
            resource temporary table name associated to the uploaded resource

        Returns
        -------
        A Job object
        """
        return (
            self.__gaiatap.
            launch_job_async(query,
                             name=name,
                             output_file=output_file,
                             output_format=output_format,
                             verbose=verbose,
                             dump_to_file=dump_to_file,
                             background=background,
                             upload_resource=upload_resource,
                             upload_table_name=upload_table_name)
            )

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
        return self.__gaiatap.load_async_job(jobid, name, verbose)

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
        return self.__gaiatap.search_async_jobs(jobfilter, verbose)

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
        return self.__gaiatap.list_async_jobs(verbose)

    def __query_object(self, coordinate, radius=None, width=None, height=None,
                       async_job=False, verbose=False):
        """Launches a job
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are
        provided
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
            query = "SELECT DISTANCE(POINT('ICRS',"\
                    "" + str(self.MAIN_GAIA_TABLE_RA) + ","\
                    "" + str(self.MAIN_GAIA_TABLE_DEC) + ")"\
                    ", POINT('ICRS'," + str(ra) + "," + str(dec) + ""\
                    ")) AS dist, * FROM " + str(self.MAIN_GAIA_TABLE) + ""\
                    " WHERE CONTAINS(POINT('ICRS'"\
                    "," + str(self.MAIN_GAIA_TABLE_RA) + ","\
                    "" + str(self.MAIN_GAIA_TABLE_DEC) + "),BOX('ICRS"\
                    "'," + str(ra) + "," + str(dec) + ", "\
                    "" + str(widthDeg.value) + ","\
                    " " + str(heightDeg.value) + ""\
                    "))=1 ORDER BY dist ASC"
            if async_job:
                job = self.__gaiatap.launch_job_async(query, verbose=verbose)
            else:
                job = self.__gaiatap.launch_job(query, verbose=verbose)
        return job.get_results()

    def query_object(self, coordinate, radius=None, width=None, height=None,
                     verbose=False):
        """Launches a job
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinates, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are
        provided
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
        return self.__query_object(coordinate, radius, width, height,
                                   async_job=False, verbose=verbose)

    def query_object_async(self, coordinate, radius=None, width=None,
                           height=None, verbose=False):
        """Launches a job (async)
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinates, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width' nor 'height' are
        provided
            radius
        width : astropy.units, required if no 'radius' is provided
            box width
        height : astropy.units, required if no 'radius' is provided
            box height
        async_job : bool, optional, default 'False'
            executes the query (job) in asynchronous/synchronous mode
            (default synchronous)
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        The job results (astropy.table).
        """
        return self.__query_object(coordinate, radius, width, height,
                                   async_job=True, verbose=verbose)

    def __cone_search(self, coordinate, radius, table_name=MAIN_GAIA_TABLE,
                      ra_column_name=MAIN_GAIA_TABLE_RA,
                      dec_column_name=MAIN_GAIA_TABLE_DEC,
                      async_job=False,
                      background=False,
                      output_file=None, output_format="votable", verbose=False,
                      dump_to_file=False):
        """Cone search sorted by distance
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        table_name: str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name: str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name: str, optional, default dec column in main gaia table
            dec column doing the cone search against
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
        query = "SELECT DISTANCE(POINT('ICRS',"+str(ra_column_name)+","\
            + str(dec_column_name)+"), \
            POINT('ICRS',"+str(ra)+","+str(dec)+")) AS dist, * \
            FROM "+str(table_name)+" WHERE CONTAINS(\
            POINT('ICRS',"+str(ra_column_name)+","+str(dec_column_name)+"),\
            CIRCLE('ICRS',"+str(ra)+","+str(dec)+", "+str(radiusDeg)+"))=1 \
            ORDER BY dist ASC"
        if async_job:
            return self.__gaiatap.launch_job_async(query=query,
                                                   output_file=output_file,
                                                   output_format=output_format,
                                                   verbose=verbose,
                                                   dump_to_file=dump_to_file,
                                                   background=background)
        else:
            return self.__gaiatap.launch_job(query=query,
                                             output_file=output_file,
                                             output_format=output_format,
                                             verbose=verbose,
                                             dump_to_file=dump_to_file)

    def cone_search(self, coordinate, radius=None,
                    table_name=MAIN_GAIA_TABLE,
                    ra_column_name=MAIN_GAIA_TABLE_RA,
                    dec_column_name=MAIN_GAIA_TABLE_DEC,
                    output_file=None,
                    output_format="votable", verbose=False,
                    dump_to_file=False):
        """Cone search sorted by distance (sync.)
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        table_name: str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name: str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name: str, optional, default dec column in main gaia table
            dec column doing the cone search against
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
                                  dump_to_file=dump_to_file)

    def cone_search_async(self, coordinate, radius=None,
                          table_name=MAIN_GAIA_TABLE,
                          ra_column_name=MAIN_GAIA_TABLE_RA,
                          dec_column_name=MAIN_GAIA_TABLE_DEC,
                          background=False,
                          output_file=None, output_format="votable",
                          verbose=False, dump_to_file=False):
        """Cone search sorted by distance (async)
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinate, mandatory
            coordinates center point
        radius : astropy.units, mandatory
            radius
        table_name: str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name: str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name: str, optional, default dec column in main gaia table
            dec column doing the cone search against
        background : bool, optional, default 'False'
            when the job is executed in asynchronous mode, this flag
            specifies whether
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
                                  dump_to_file=dump_to_file)

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
        return self.__gaiatap.remove_jobs(jobs_list)

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
        return self.__gaiatap.save_results(job, verbose)

    def login(self, user=None, password=None, credentials_file=None,
              verbose=False):
        """Performs a login.
        TAP+ only
        User and password can be used or a file that contains user name and
        password
        (2 lines: one for user name and the following one for the password)

        Parameters
        ----------
        user : str, mandatory if 'file' is not provided, default None
            login name
        password : str, mandatory if 'file' is not provided, default None
            user password
        credentials_file : str, mandatory if no 'user' & 'password' are
        provided
            file containing user and password in two lines
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__gaiatap.login(user=user,
                                    password=password,
                                    credentials_file=credentials_file,
                                    verbose=verbose)

    def login_gui(self, verbose=False):
        """Performs a login using a GUI dialog
        TAP+ only

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__gaiatap.login_gui(verbose)

    def logout(self, verbose=False):
        """Performs a logout
        TAP+ only

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        return self.__gaiatap.logout(verbose)

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
        if not (isinstance(value, str) or isinstance(value,
                                                     commons.CoordClasses)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")

    def __getCoordInput(self, value, msg):
        if not (isinstance(value, str) or isinstance(value,
                                                     commons.CoordClasses)):
            raise ValueError(
                str(msg) + " must be either a string or astropy.coordinates")
        if isinstance(value, str):
            c = commons.parse_coordinates(value)
            return c
        else:
            return value

    def upload_table(self, upload_resource=None, table_name=None,
                     table_description=None,
                     format=None, verbose=False):
        """Uploads a table to the user private space

        Parameters
        ----------
        upload_resource : str, mandatory
            resource to be uploaded (table or URL)
        table_name: str, required if uploadResource is provided, default None
            resource temporary table name associated to the uploaded resource
        table_description: str, optional, default None
            table description
        format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.upload_table(
            upload_resource=upload_resource,
            table_name=table_name,
            table_description=table_description,
            format=format, verbose=verbose)

    def upload_table_from_job(self, job=None, verbose=False):
        """Uploads a table to the user private space from a job

        Parameters
        ----------
        job : job, mandatory
            job used to create a table
        table_name: str, required if uploadResource is provided, default None
            resource temporary table name associated to the uploaded resource
        table_description: str, optional, default None
            table description
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """

        return self.__gaiatap.upload_table_from_job(job, verbose)

    def update_user_table(self, table_name=None, list_of_changes=[],
                          verbose=False):
        """Updates a user table

        Parameters
        ----------
        table_name: str, required
            table to be updated
        list_of_changes: list, required
            list of lists, each one of them containing sets of
            [column_name, field_name, value].
            column_name is the name of the column to be updated
            field_name is the name of the tap field to be modified
            field name can be 'utype', 'ucd', 'flags' or 'indexed'
            value is the new value this field of this column will take
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.update_user_table(table_name,
                                                list_of_changes,
                                                verbose)

    def set_ra_dec_columns(self, table_name=None,
                           ra_column_name=None, dec_column_name=None,
                           verbose=False):
        """Set columns of a table as ra and dec respectively a user table

        Parameters
        ----------
        table_name: str, required
            table to be set
        ra_column_name: str, required
            ra column to be set
        dec_column_name: str, required
            dec column to be set
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """

        return self.__gaiatap.set_ra_dec_columns(table_name,
                                                 ra_column_name,
                                                 dec_column_name,
                                                 verbose)

    def delete_user_table(self, table_name=None, force_removal=False,
                          verbose=False):
        """Removes a user table

        Parameters
        ----------
        table_name: str, required
            table to be removed
        force_removal : bool, optional, default 'False'
            flag to indicate if removal should be forced
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.delete_user_table(
            table_name=table_name, force_removal=force_removal,
            verbose=verbose)
        
    def load_groups(self, verbose=False):
        """Loads groups

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A set of groups of a user
        """
        return self.__gaiatap.load_groups(verbose=verbose)

    def load_shared_items(self, verbose=False):
        """Loads shared items

        Parameters
        ----------
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A set of shared items
        """
        return self.__gaiatap.load_shared_items(verbose=verbose)

    def share_table(self, group_name=None,
                    table_name=None,
                    description=None,
                    verbose=False):
        """Shares a table with a group

        Parameters
        ----------
        group_name: str, required
            group in which table will be shared
        table_name: str, required
            table to be shared
        description: str, required
            description of the sharing
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_table(group_name=group_name,
                                          table_name=table_name,
                                          description=description,
                                          verbose=verbose)
        
    def share_table_stop(self,
                    table_name=None,
                    verbose=False):
        """Stop sharing a table

        Parameters
        ----------
        table_name: str, required
            table to be stopped from being shared
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_table_stop(table_name=table_name,
                                        verbose=verbose)
        
    def share_group_create(self,
                    group_name=None,
                    description=None,
                    verbose=False):
        """Creates a group

        Parameters
        ----------
        group_name: str, required
            group to be created
        description: str, required
            description of the group
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_group_create(group_name=group_name,
                                          description=description,
                                          verbose=verbose)
    
    def share_group_delete(self,
                    group_name=None,
                    verbose=False):
        """Deletes a group

        Parameters
        ----------
        group_name: str, required
            group to be created
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_group_delete(group_name=group_name,
                                          verbose=verbose)

    def share_group_add_user(self,
                             group_name=None,
                             user_id=None,
                             verbose=False):
        """Adds user to a group

        Parameters
        ----------
        group_name: str, required
            group which user_id will be added in
        user_id: str, required
            user id to be added
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_group_add_user(group_name=group_name,
                                                   user_id=user_id,
                                                   verbose=verbose)
      
    def share_group_delete_user(self,
                                group_name=None,
                                user_id=None,
                                verbose=False):
        """Deletes user from a group

        Parameters
        ----------
        group_name: str, required
            group which user_id will be removed from
        user_id: str, required
            user id to be deleted
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A message (OK/Error) or a job when the table is big
        """
        return self.__gaiatap.share_group_delete_user(group_name=group_name,
                                                      user_id=user_id,
                                                      verbose=verbose)
      
    def load_user(self, user_id, verbose=False):
        """Loads the specified user
        TAP+ only

        Parameters
        ----------
        user_id : str, mandatory
            user id to load
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A user
        """
        

Gaia = GaiaClass()
