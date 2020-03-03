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
import six
import zipfile
from astroquery.utils.tap import taputils
from . import conf
import os
from datetime import datetime
import shutil
import astroquery.utils.tap.model.modelutils as modelutils
from astropy.io.votable import parse
from astropy.io.votable.tree import VOTableFile, Resource, Field
from astropy.table import Table

class GaiaClass(TapPlus):

    """
    Proxy class to default TapPlus object (pointing to Gaia Archive)
    """
    MAIN_GAIA_TABLE = conf.MAIN_GAIA_TABLE
    MAIN_GAIA_TABLE_RA = conf.MAIN_GAIA_TABLE_RA
    MAIN_GAIA_TABLE_DEC = conf.MAIN_GAIA_TABLE_DEC

    def __init__(self, tap_plus_conn_handler=None,
                 datalink_handler=None,
                 gaia_tap_server='http://gea.esac.esa.int/',
                 gaia_data_server='http://gea.esac.esa.int/'):
        
        super(GaiaClass, self).__init__(url=gaia_tap_server,
                                        server_context="tap-server",
                                        tap_context="tap",
                                        upload_context="Upload",
                                        table_edit_context="TableTool",
                                        data_context="data",
                                        datalink_context="datalink",
                                        connhandler=tap_plus_conn_handler)
        # Data uses a different TapPlus connection
        if datalink_handler is None:
            self.__gaiadata = TapPlus(url=gaia_data_server,
                                      server_context="data-server",
                                      tap_context="tap",
                                      upload_context="Upload",
                                      table_edit_context="TableTool",
                                      data_context="data",
                                      datalink_context="datalink")
        else:
            self.__gaiadata = datalink_handler

    def dual_login(self, data=None, verbose=False):
        if data is None:
            data = self.__gaiadata

        correct = True
        try:
            TapPlus.login(self, verbose=verbose)
        except:
            print ("Error logging in tap")
            correct = False
        if correct == True:
            try:
                TapPlus.login(data, verbose=verbose)
            except:
                print ("Error logging in data")
                print ("Logging out from tap")
                TapPlus.logout(self, verbose=verbose)

    def dual_login_gui(self, data=None, verbose=False):
        if data is None:
            data = self.__gaiadata

        correct = True
        try:
            TapPlus.login_gui(self, verbose=verbose)
        except:
            print ("Error logging in tap")
            correct = False
        if correct == True:
            try:
                TapPlus.login_gui(data, verbose=verbose)
            except:
                print ("Error logging in data")
                print ("Logging out from tap")
                TapPlus.logout(self, verbose=verbose)

    def dual_logout(self, data=None, verbose=False):
        if data is None:
            data = self.__gaiadata

        correct = True
        try:
            TapPlus.logout(self, verbose=verbose)
        except:
            print ("Error logging out tap")
            correct = False
        if correct == True:
            print("OK")
            try:
                TapPlus.logout(data, verbose=verbose)
            except:
                correct = False
                print ("Error logging out data")
        if correct == True:
            print("OK")

    def load_data(self,
                  ids,
                  data_release=None,
                  data_structure='INDIVIDUAL',
                  retrieval_type="ALL",
                  valid_data=True,
                  band=None,
                  format="votable",
                  output_file=None,
                  verbose=False):
        """Loads the specified table
        TAP+ only

        Parameters
        ----------
        ids : str list, mandatory
            list of identifiers
        data_release: integer, optional, default None
            data release from which data should be taken
        data_structure: str, optional, default 'INDIVIDUAL'
            it can be 'INDIVIDUAL', 'COMBINED', 'RAW':
            'INDIVIDUAL' means...
            'COMBINED' means...
            'RAW' means...
        retrieval_type : str, optional, default 'ALL'
            retrieval type identifier. It can be either 'epoch_photometry'
            for compatibility reasons or 'ALL' to retrieve all data from
            the list of sources.
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
        output_file : string, optional, default None
            file where the results are saved.
            If it is not provided, the http response contents are returned.
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        A table object
        """
        if retrieval_type is None:
            raise ValueError("Missing mandatory argument 'retrieval_type'")
        now = datetime.now()
        output_file_specified = False;
        if output_file is None:
            output_file = os.getcwd() + os.sep + "temp_" + now.strftime("%Y%m%d%H%M%S") + os.sep + "download_" + now.strftime("%Y%m%d%H%M%S")
        else:
            output_file_specified = True
            output_file = os.getcwd() + os.sep + "temp_" + now.strftime("%Y%m%d%H%M%S") + os.sep + output_file
        path = os.path.dirname(output_file)
        try:
            os.mkdir(path)
        except OSError:
            print ("Creation of the directory %s failed" % path)

        if ids is None:
            raise ValueError("Missing mandatory argument 'ids'")
        if str(retrieval_type) != 'ALL' and str(retrieval_type) != 'epoch_photometry':
            raise ValueError("Invalid mandatory argument 'retrieval_type'")

        params_dict = {}
        if not valid_data or str(retrieval_type) == 'ALL':
            params_dict['VALID_DATA'] = "false"
        elif valid_data:
            params_dict['VALID_DATA'] = "true"

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
        if data_release is not None:
            params_dict['DATA_RELEASE'] = data_release
        params_dict['DATA_STRUCTURE'] = data_structure
        params_dict['FORMAT'] = str(format)
        params_dict['RETRIEVAL_TYPE'] = str(retrieval_type)
        self.__gaiadata.load_data(params_dict=params_dict,
                                         output_file=output_file,
                                         verbose=verbose)

        files = {}
        if zipfile.is_zipfile(output_file):  
            with zipfile.ZipFile(output_file, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(output_file))

        #r=root, d=directories, f = files
        for r, d, f in os.walk(path):
            for file in f:
                if '.fits' in file or '.xml' in file or '.csv' in file:
                    files[file] = os.path.join(r, file)

        for key,value in files.items():
            if '.fits' in key or '.csv' in key:
                files[key] = modelutils.read_results_table_from_file(value, format)
            elif '.xml' in key:
                tables = []
                for table in parse(value).iter_tables():
                    tables.append(table)
                files[key] = tables

        if not output_file_specified:
            shutil.rmtree(path)
        else:
            print("output_file =", output_file)

        print("List of products available:")
        for key,value in files.items():
            print("Product =", key)

        return files

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
                job = self.launch_job_async(query, verbose=verbose)
            else:
                job = self.launch_job(query, verbose=verbose)
        return job.get_results()

    def query_object(self, coordinate, radius=None, width=None, height=None,
                     verbose=False):
        """Launches a job
        TAP & TAP+

        Parameters
        ----------
        coordinate : astropy.coordinates, mandatory
            coordinates center point
        radius : astropy.units, required if no 'width'/'height' are provided
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
        radius : astropy.units, required if no 'width'/'height' are provided
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
        table_name : str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name : str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name : str, optional, default dec column in main gaia table
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
            return self.launch_job_async(query=query,
                                         output_file=output_file,
                                         output_format=output_format,
                                         verbose=verbose,
                                         dump_to_file=dump_to_file,
                                         background=background)
        else:
            return self.launch_job(query=query,
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
        table_name : str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name : str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name : str, optional, default dec column in main gaia table
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
        table_name : str, optional, default main gaia table
            table name doing the cone search against
        ra_column_name : str, optional, default ra column in main gaia table
            ra column doing the cone search against
        dec_column_name : str, optional, default dec column in main gaia table
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

        return self.is_valid_user(user_id=user_id,
                                            verbose=verbose)

    def cross_match(self, full_qualified_table_name_a=None,
                    full_qualified_table_name_b=None,
                    results_table_name=None,
                    radius=1.0,
                    background=False,
                    verbose=False):
        """Performs a cross match between the specified tables
        The result is a join table (stored in the user storage area)
        with the identifies of both tables and the distance.
        TAP+ only

        Parameters
        ----------
        full_qualified_table_name_a : str, mandatory
            a full qualified table name (i.e. schema name and table name)
        full_qualified_table_name_b : str, mandatory
            a full qualified table name (i.e. schema name and table name)
        results_table_name : str, mandatory
            a table name without schema. The schema is set to the user one
        radius : float (arc. seconds), optional, default 1.0
            radius  (valid range: 0.1-10.0)
        verbose : bool, optional, default 'False'
            flag to display information about the process

        Returns
        -------
        Boolean indicating if the specified user is valid
        """
        if full_qualified_table_name_a is None:
            raise ValueError("Table name A argument is mandatory")
        if full_qualified_table_name_b is None:
            raise ValueError("Table name B argument is mandatory")
        if results_table_name is None:
            raise ValueError("Results table name argument is mandatory")
        if radius < 0.1 or radius > 10.0:
            raise ValueError("Invalid radius value. Found " + str(radius) +
                             ", valid range is: 0.1 to 10.0")
        schemaA = taputils.get_schema_name(full_qualified_table_name_a)
        if schemaA is None:
            raise ValueError("Not found schema name in " +
                             "full qualified table A: '" +
                             full_qualified_table_name_a + "'")
        tableA = taputils.get_table_name(full_qualified_table_name_a)
        schemaB = taputils.get_schema_name(full_qualified_table_name_b)
        if schemaB is None:
            raise ValueError("Not found schema name in " +
                             "full qualified table B: '" +
                             full_qualified_table_name_b + "'")
        tableB = taputils.get_table_name(full_qualified_table_name_b)
        if taputils.get_schema_name(results_table_name) is not None:
            raise ValueError("Please, do not specify schema for " +
                             "'results_table_name'")
        query = "SELECT crossmatch_positional(\
            '"+schemaA+"','"+tableA+"',\
            '"+schemaB+"','"+tableB+"',\
            "+str(radius)+",\
            '"+str(results_table_name)+"')\
            FROM dual;"
        name = str(results_table_name)
        return self.launch_job_async(query=query,
                                     name=name,
                                     output_file=None,
                                     output_format="votable",
                                     verbose=verbose,
                                     dump_to_file=False,
                                     background=background,
                                     upload_resource=None,
                                     upload_table_name=None)


Gaia = GaiaClass()
