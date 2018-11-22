# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
TAP plus
=============

@author: Juan Carlos Segovia
@contact: juan.carlos.segovia@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 30 jun. 2016


"""
from astroquery.utils.tap import taputils
from astroquery.utils.tap.conn.tapconn import TapConn
from astroquery.utils.tap.xmlparser.tableSaxParser import TableSaxParser
from astroquery.utils.tap.model.job import Job
from datetime import datetime
from astroquery.utils.tap.gui.login import LoginDialog
from astroquery.utils.tap.xmlparser.jobSaxParser import JobSaxParser
from astroquery.utils.tap.xmlparser.jobListSaxParser import JobListSaxParser
from astroquery.utils.tap.xmlparser.groupSaxParser import GroupSaxParser
from astroquery.utils.tap.xmlparser.sharedItemsSaxParser import SharedItemsSaxParser  # noqa
from astroquery.utils.tap.xmlparser import utils
from astroquery.utils.tap.model.filter import Filter
import six
import requests
import getpass
import os
from astropy.table.table import Table
import tempfile

import io
import requests
from astropy.extern.six.moves.urllib_parse import urljoin, urlparse
import logging
logger = logging.getLogger(__name__)

__all__ = ['Tap', 'TapPlus']

VERSION = "1.2.1"
TAP_CLIENT_ID = "aqtappy-" + VERSION


class Tap(object):
    """TAP class
    Provides TAP capabilities
    """

    def __init__(self, host, path, protocol='http', port=80):
        """Constructor

        Parameters
        ----------
        host : str, required
            host name
        path : str, mandatory
            server context
        protocol : str, optional
            access protocol, usually 'http' or 'https'
        port : int, optional, default '80'
            HTTP port
        """
        self.protocol = protocol
        self.host = host
        self.path = path
        self.port = port
        self.session = requests.session()

        logger.debug('TAP: {:s}'.format(self.tap_endpoint))
    
    @property
    def tap_endpoint(self):
        return urljoin("{s.protocol:s}://{s.host:s}".format(s=self), self.path)

    # TODO: make this a cached property?
    # TODO: actually found this to return private tables, too when logged in.
    def load_tables(self):
        """Loads all public tables

        Returns
        -------
        A list of table objects
        """
        response = self.session.get("{s.tap_endpoint}/tables".format(s=self))
        if not response.raise_for_status():
            tsp = TableSaxParser()
            # TODO: this is a stopgap
            tsp.parseData(io.BytesIO(response.content))
            return tsp.get_tables()

    def load_table(self, table):
        """Loads the specified table

        Parameters
        ----------
        table : str, mandatory
            full qualified table name (i.e. schema name + table name)

        Returns
        -------
        A table object
        """
        url = "{s.tap_endpoint:s}/tables?tables={table:s}".format(
            s=self, table=table)
        response = self.session.get(url)
        if not response.raise_for_status():
            tsp = TableSaxParser()
            tsp.parseData(io.BytesIO(response.content))
            return tsp.get_table()

    # TODO: clean up all dump_to_file leftover
    def _post_query(self, query, name=None, output_file=None,
                   output_format="votable", verbose=False,
                   upload_resource=None, upload_table_name=None,
                   async_=False,
                   dump_to_file=False, autorun=True):
        """POST synchronous or asynchronos query to Tap server

        Parameters
        ----------
        query : str, mandatory
            query to be executed
        output_file : str, optional, default None
            file name where the results are saved if dump_to_file is True.
            If this parameter is not provided, the jobid is used instead
        output_format : str, optional, default 'votable'
            results format
        verbose : bool, optional, default 'False'
            flag to display information about the process
        async_ : bool
            send asynchronous query if True
        dump_to_file : bool, optional, default 'False'
            if True, the results are saved in a file instead of using memory
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if upload_resource is
            provided, default None
            resource temporary table name associated to the uploaded resource

        Returns
        -------
        response : requests.Response
        """
        # TODO: docstring is missing what other output_format is available.
        # TODO: I suggest to delegate output manipulation to users
        #       and just return astropy Table.
        query = taputils.set_top_in_query(query, 2000)

        args = {
            "REQUEST": "doQuery",
            "LANG": "ADQL",
            "FORMAT": str(output_format),
            "tapclient": str(TAP_CLIENT_ID),
            "QUERY": str(query)}
        # TODO: is autorun even necessary for sync jobs?
        if autorun is True:
            args['PHASE'] = 'RUN'
        if name is not None:
            args['jobname'] = name
        url = self.tap_endpoint + ('/async' if async_ else '/sync')
        
        if upload_resource is None:
            response = self.session.post(url, data=args)
        else:
            if upload_table_name is None:
                raise ValueError("Table name is required when a resource " +
                                 "is uploaded")
            # UPLOAD should be '[table_name],param:form_key'
            args['UPLOAD'] = '{0:s},param:{0:s}'.format(upload_table_name)
            if isinstance(upload_resource, Table):
                with io.BytesIO() as f:
                    upload_resource.write(f, format='votable')
                    f.seek(0)
                    chunk = f.read()
                    name = 'pytable'
                    args['format'] = 'votable'
            else:
                with open(upload_resource, "r") as f:
                    chunk = f.read()
                name = os.path.basename(upload_resource)
            # TODO: It is possible to set content type explicitly but
            #       it is not sure if that's necessary.
            #       If not, remove variable `name`.
            files = {upload_table_name: chunk}
            response = self.session.post(url, data=args, files=files)

        if not response.raise_for_status():
            return response
    
    def query(self, query, name=None, upload_resource=None, upload_table_name=None):
        """
        Synchronous query to TAP server
        """
        r = self._post_query(query, name=name, upload_resource=upload_resource,
                             upload_table_name=upload_table_name)
        #TODO parse response and return results
        return r

    def query_async(self, query, name=None, output_file=None,
                         output_format="votable", verbose=False,
                         dump_to_file=False, background=False,
                         upload_resource=None, upload_table_name=None,
                         autorun=True):
        """Launches an asynchronous job

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
            whether the execution will wait until results are available
        upload_resource: str, optional, default None
            resource to be uploaded to UPLOAD_SCHEMA
        upload_table_name: str, required if upload_resource is
            provided, default None
            resource temporary table name associated to the uploaded resource
        autorun: boolean, optional, default True
            if 'True', sets 'phase' parameter to 'RUN',
            so the framework can start the job.

        Returns
        -------
        A Job object
        """
        r = self._post_query(query, name=name, async_=True)
        # TODO: parse response and return Job
        return r

    @classmethod
    def from_url(cls, url):
        """
        Make a Tap from url [http(s)://]host[:port][/path]
        """
        default_port = {'http': 80, 'https':443}
        if '://' not in url:
            raise ValueError('`url` must start with "scheme://"')
        parsed_url = urlparse(url)
        protocol = parsed_url.scheme
        host = parsed_url.hostname
        port = parsed_url.port
        if not port:
            port = default_port[protocol]
        path = parsed_url.path
        return cls(host, path, protocol=protocol, port=port)

    def __repr__(self):
        return '{cls:s}("{s.host:s}", "{s.path:s}", "{s.protocol:s}", {s.port:d})'\
            .format(s=self, cls=self.__class__.__name__)

    def __str__(self):
        return ("Created TAP+ (v"+VERSION+") - Connection:\n" +
                str(self.connhandler))


class TapPlus(Tap):
    """TAP plus class
    Provides TAP and TAP+ capabilities
    """
    def __init__(self, host, path, protocol='http', port=80,
                 server_context=None, upload_context=None):
                #  table_edit_context=None, data_context=None,
                #  datalink_context=None):
        """Constructor

        Parameters
        ----------
        host : str, optional, default None
            host name
        server_context : str, optional, default None
            server context
        upload_context : str, optional, default None
            upload context
        table_edit_context : str, optional, default None
            context for all actions to be performed over a existing table
        data_context : str, optional, default None
            data context
        datalink_context : str, optional, default None
            datalink context
        verbose : bool, optional, default 'True'
            flag to display information about the process
        """

        super(TapPlus, self).__init__(host, path, protocol=protocol, port=port)

        if not all([v is not None for v in [server_context, upload_context]]):
            raise ValueError(
                "It does not make sense to initialize `TapPlus`"
                "without all contexts set. Consider using `Tap`.")

        self.server_context = server_context
        self.upload_context = upload_context
        # self.table_edit_context = table_edit_context
        # self.data_context = data_context
        # self.datalink_context = datalink_context
    
    def login(self, user=None, password=None, credentials_file=None,
              verbose=False):
        """
        Login to TAP server

        User and password arguments can be used or a file that contains user
        name and password (2 lines: one for user name and the following one
        for the password). If no arguments are provided, a prompt asking for
        user name and password will appear.

        Parameters
        ----------
        user : str, default None
            login name
        password : str, default None
            user password
        credentials_file : str, default None
            file containing user and password in two lines
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        if credentials_file is not None:
            # read file: get user & password
            with open(credentials_file, "r") as ins:
                user = ins.readline().strip()
                password = ins.readline().strip()
        if user is None:
            user = six.moves.input("User: ")
            if user is None:
                print("Invalid user name")
                return
        if password is None:
            password = getpass.getpass("Password: ")
            if password is None:
                print("Invalid password")
                return
        url = "https://{s.host:s}/{s.server_context:s}/login".format(s=self)
        r = self.session.post(url, data={'username': user, 'password': password})
        if not r.raise_for_status():
            return
    
    def logout(self):
        """
        Logout from TAP server
        """
        url = "https://{s.host:s}/{s.server_context:s}/logout".format(s=self)
        r = self.session.post(url)
        if not r.raise_for_status():
            return
    
    @property
    def baseurl(self):
        return '{s.protocol:s}://{s.host:s}/{s.server_context:s}'.format(s=self)

    def load_tables(self, only_tables=False, include_shared_tables=False):
        """
        Load all accessible tables

        Parameters
        ----------
        only_tables : bool, TAP+ only, optional, default 'False'
            True to load table names only
        include_shared_tables : bool, TAP+, optional, default 'False'
            True to include shared tables

        Returns
        -------
        A list of table objects
        """
        url = "{s.tap_endpoint}/tables".format(s=self)
        logger.debug("tables url = {:s}".format(url))

        payload = dict(
            only_tables=only_tables,
            share_accessible=True if include_shared_tables else False
        )
        r = self.session.get(url, params=payload)

        if not r.raise_for_status():
            tsp = TableSaxParser()
            # TODO: this is a stopgap
            tsp.parseData(io.BytesIO(r.content))
        return tsp.get_tables()

    def upload_table(self, upload_resource, table_name,
                     table_description="",
                     format='votable'):
        """
        Upload a table to the user private space

        Parameters
        ----------
        upload_resource : object
            table to be uploaded: pyTable, file or URL.
        table_name: str
            table name associated to the uploaded resource
        table_description: str, optional
            table description
        format : str, optional
            resource format
            Available formats: 'VOTable', 'CSV' and 'ASCII'
        """
        # TODO: available froamts from http://gea.esac.esa.int/archive-help/index.html
        #       where else is TapPlus applicable?
        url = "{s.baseurl:s}/{s.upload_context}".format(s=self)
        # url = "https://gea.esac.esa.int/tap-server/Upload"
        logger.debug("upload_table url = {:s}".format(url))
        # TODO: WOW. It actaully seems necessary to pass on TASKID
        # Otherwise you will get 500 Internal server error.
        # This is confirmed even with curl in command line.
        # This is not docummented in gaia help page.
        args = {
            'TASKID': str(-1),
            'TABLE_NAME': table_name,
            'TABLE_DESC': table_description,
            'FORMAT': format
        }
        if isinstance(upload_resource, Table):
            with io.BytesIO() as f:
                upload_resource.write(f, format='votable')
                f.seek(0)
                chunk = f.read()
                args['FORMAT'] = 'votable'
                files = dict(FILE=chunk)
        elif upload_resource.startswith('http'):
            files = None
            args['URL'] = upload_resource
        else:
            with open(upload_resource, "r") as f:
                chunk = f.read()
            files = dict(FILE=chunk)
        r = self.session.post(url, data=args, files=files)
        return r

    def delete_user_table(self, table_name, force_removal=False,
                          verbose=False):
        """
        Remove a user table

        Parameters
        ----------
        table_name: str
            table to be removed
        force_removal : bool, optional, default 'False'
            flag to indicate if removal should be forced
        verbose : bool, optional, default 'False'
            flag to display information about the process
        """
        url = "{s.baseurl:s}/{s.upload_context}".format(s=self)

        # TODO: what does force removal mean?
        args = {
            "TABLE_NAME": str(table_name),
            "DELETE": "TRUE",
            "FORCE_REMOVAL": "TRUE" if force_removal else "FALSE"
        }

        r = self.session.post(url, data=args)
        if not r.raise_for_status():
            return r