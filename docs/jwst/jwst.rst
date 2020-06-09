.. doctest-skip-all

.. _astroquery.jwst:

*****************************
JWST TAP+ (`astroquery.jwst`)
*****************************

The James Webb Space Telescope (JWST) is a collaborative project between NASA, 
ESA, and the Canadian Space Agency (CSA). Although radically different in 
design, and emphasizing the infrared part of the electromagnetic spectrum, 
JWST is widely seen as the successor to the Hubble Space Telescope (HST). 
The JWST observatory will consist of a deployable 6.6 meter passively cooled 
telescope optimized for infrared wavelengths, and will be operated in deep 
space at the anti-Sun Earth-Sun Lagrangian point (L2). It will carry four 
scientific instruments: a near-infrared camera (NIRCam), a 
near-infrared multi-object spectrograph (NIRSpec) covering the 0.6 - 5 μm 
spectral region, a near-infrared slit-less spectrograph (NIRISS), and a 
combined mid-infrared camera/spectrograph (MIRI) covering 5 - 28 μm. The JWST 
focal plane (see image to the right) contains apertures for the science 
instruments and the Fine Guidance Sensor (FGS). 

The scientific goals of the JWST mission can be sorted into four broad themes: 
The birth of stars and proto-planetary systems Planetary systems and the 
origins of life

* The end of the dark ages: first light and re-ionization.
* The assembly of galaxies.
* The birth of stars and proto-planetary systems.
* Planetary systems and the origins of life.

This package allows the access to the European Space Agency JWST Archive
(http://jwst.esac.esa.int/archive/)

ESA JWST Archive access is based on a TAP+ REST service. TAP+ is an extension of
Table Access Protocol (TAP: http://www.ivoa.net/documents/TAP/) specified by the
International Virtual Observatory Alliance (IVOA: http://www.ivoa.net).

The TAP query language is Astronomical Data Query Language
(ADQL: http://www.ivoa.net/documents/ADQL/2.0), which is similar
to Structured Query Language (SQL), widely used to query databases.

TAP provides two operation modes: Synchronous and Asynchronous:

* Synchronous: the response to the request will be generated as soon as the
  request received by the server.
  (Do not use this method for queries that generate a big amount of results.)
* Asynchronous: the server will start a job that will execute the request.
  The first response to the request is the required information (a link)
  to obtain the job status.
  Once the job is finished, the results can be retrieved.

ESA JWST TAP+ server provides two access mode: public and authenticated:

* Public: this is the standard TAP access. A user can execute ADQL queries and
  upload tables to be used in a query 'on-the-fly' (these tables will be removed
  once the query is executed). The results are available to any other user and
  they will remain in the server for a limited space of time.

* Authenticated: some functionalities are restricted to authenticated users only.
  The results are saved in a private user space and they will remain in the 
  server for ever (they can be removed by the user).

  * ADQL queries and results are saved in a user private area.

  * Cross-match operations: a catalog cross-match operation can be executed.
    Cross-match operations results are saved in a user private area.

  * Persistence of uploaded tables: a user can upload a table in a private space.
    These tables can be used in queries as well as in cross-matches operations.


This python module provides an Astroquery API access. Nevertheless, only
``query_region`` and ``query_region_async`` are implemented.


========
Examples
========

---------------------------
1. Non authenticated access
---------------------------

1.1. Query region
~~~~~~~~~~~~~~~~~

.. code-block:: python

  >>> import astropy.units as u
  >>> from astropy.coordinates import SkyCoord
  >>> from astroquery.jwst import Jwst
  >>>
  >>> coord = SkyCoord(ra=53, dec=-27, unit=(u.degree, u.degree), frame='icrs')
  >>> width = u.Quantity(5, u.deg)
  >>> height = u.Quantity(5, u.deg)
  >>> r = Jwst.query_region_async(coordinate=coord, width=width, height=height)
  >>> r.pprint()

  Query finished.
         dist                       obsid                 ...  type typecode
  ------------------ ------------------------------------ ... ----- --------
  0.8042331552744052 00000000-0000-0000-8f43-c68be243b878 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-8f43-c68be243b878 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-94fc-23f102d345d3 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-94fc-23f102d345d3 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-a288-14744c2a684b ... PRIME        S
  0.8042331552744052 00000000-0000-0000-a288-14744c2a684b ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3cc-6aa1e2e509c2 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3cc-6aa1e2e509c2 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3eb-870a80410d40 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3eb-870a80410d40 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-babe-5c1ec63d3301 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-babe-5c1ec63d3301 ... PRIME        S


1.2. Cone search
~~~~~~~~~~~~~~~~

.. code-block:: python

  >>> import astropy.units as u
  >>> from astropy.coordinates import SkyCoord
  >>> from astroquery.Jwst import Jwst
  >>>
  >>> coord = SkyCoord(ra=53, dec=-27, unit=(u.degree, u.degree), frame='icrs')
  >>> radius = u.Quantity(5.0, u.deg)
  >>> j = Jwst.cone_search_async(coord, radius)
  >>> r = j.get_results()
  >>> r.pprint()

         dist                       obsid                 ...  type typecode
  ------------------ ------------------------------------ ... ----- --------
  0.8042331552744052 00000000-0000-0000-8f43-c68be243b878 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-8f43-c68be243b878 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-94fc-23f102d345d3 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-94fc-23f102d345d3 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-a288-14744c2a684b ... PRIME        S
  0.8042331552744052 00000000-0000-0000-a288-14744c2a684b ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3cc-6aa1e2e509c2 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3cc-6aa1e2e509c2 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3eb-870a80410d40 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-b3eb-870a80410d40 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-babe-5c1ec63d3301 ... PRIME        S
  0.8042331552744052 00000000-0000-0000-babe-5c1ec63d3301 ... PRIME        S

1.3 Getting data products
~~~~~~~~~~~~~~~~~~~~~~~~~
To query the data products associated with a certain plane ID

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> product_list = Jwst.get_product_list('00000000-0000-0000-aa9c-541cc6e5ff87')
  >>> print(product_list.group_by(['artifactid', 'filename']).groups.keys)

               artifactid                               filename                
  ------------------------------------ -----------------------------------------
  00000000-0000-0000-81df-1e50349b9801                 jw10006010001_01_msa.fits
  00000000-0000-0000-95e8-4d5c47e0d19f  jw10006010001_01101_00002_nrs2_uncal.jpg
  00000000-0000-0000-af91-b0375f07283b jw10006010001_01101_00002_nrs2_uncal.fits

To query the science data products associated with a certain plane ID

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> product_list = Jwst.get_product_list('00000000-0000-0000-aa9c-541cc6e5ff87', 'science')
  >>> print(product_list.group_by(['artifactid', 'filename']).groups.keys)

               artifactid                               filename                
 ------------------------------------ -----------------------------------------
  00000000-0000-0000-af91-b0375f07283b jw10006010001_01101_00002_nrs2_uncal.fits

To download a data product

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> product_list = Jwst.get_product('00000000-0000-0000-af91-b0375f07283b')

1.4 Getting public tables
~~~~~~~~~~~~~~~~~~~~~~~~~

To load only table names (TAP+ capability)

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> tables = Jwst.load_tables(only_names=True)
  >>> for table in (tables):
  >>>   print(table.name)

  public.dual
  tap_schema.columns
  tap_schema.key_columns
  tap_schema.keys
  tap_schema.schemas
  tap_schema.tables
  jwst.artifact
  jwst.chunk
  jwst.main
  jwst.observation
  jwst.observationmember
  jwst.part
  jwst.plane
  jwst.plane_inputs

To load table names (TAP compatible)

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> tables = Jwst.load_tables()
  >>> for table in (tables):
  >>>   print(table.name)

  public.dual
  tap_schema.columns
  tap_schema.key_columns
  tap_schema.keys
  tap_schema.schemas
  tap_schema.tables
  jwst.artifact
  jwst.chunk
  jwst.main
  jwst.observation
  jwst.observationmember
  jwst.part
  jwst.plane
  jwst.plane_inputs

To load only a table (TAP+ capability)

.. code-block:: python

  >>> from astroquery.gaia import Jwst
  >>> table = Jwst.load_table('jwst.main')
  >>> print(table)

  TAP Table name: jwst.main
  Description: 
  Num. columns: 112


Once a table is loaded, columns can be inspected

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> table = Jwst.load_table('jwst.main')
  >>> for column in (table.columns):
  >>>   print(column.name)

  obsid
  planeid
  public
  calibrationlevel
  dataproducttype
  algorithm_name
  collection
  creatorid
  energy_bandpassname
  ...
  time_exposure
  time_resolution
  time_samplesize
  type
  typecode

1.5 Synchronous query
~~~~~~~~~~~~~~~~~~~~~

A synchronous query will not store the results at server side. These queries 
must be used when the amount of data to be retrieve is 'small'.

There is a limit of 2000 rows. If you need more than that, you must use 
asynchronous queries.

The results can be saved in memory (default) or in a file.

Query without saving results in a file:

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>>
  >>> job = Jwst.launch_job("SELECT TOP 100 \
  >>> instrument_name, observationuri, planeid, calibrationlevel, \
  >>> dataproducttype, targetposition_coordinates_cval1 as target_ra, \
  >>> targetposition_coordinates_cval2 as target_dec \
  >>> FROM jwst.main ORDER BY instrument_name, observationuri")
  >>>
  >>> print(job)

  Jobid: None
  Phase: COMPLETED
  Owner: None
  Output file: sync_20170223111452.xml.gz
  Results: None

  >>> r = job.get_results()
  >>> print(r['planeid'])

                planeid               
  ------------------------------------
  00000000-0000-0000-9d6d-f192fde74ce4
  00000000-0000-0000-8a85-d34d6a411611
  00000000-0000-0000-969c-a49226673efa
  00000000-0000-0000-8c07-c26c24bec2ee
  00000000-0000-0000-89d2-b42624493c84
  00000000-0000-0000-800d-659917e7bb26
  00000000-0000-0000-8cb6-748fa37d47e3
  00000000-0000-0000-8573-92ad575b8fb4
  00000000-0000-0000-8572-b7b226953a2c
  00000000-0000-0000-8d1d-765c362e3227
                                   ...
  00000000-0000-0000-b7d9-b4686ed37bf0
  00000000-0000-0000-822f-08376ffe6f0b
  00000000-0000-0000-8a8e-8cd48bb4cd7a
  00000000-0000-0000-8a9d-3e1aae1281ba
  00000000-0000-0000-a2ac-1ac288320bf7
  00000000-0000-0000-a20f-835a58ca7872
  00000000-0000-0000-aa9c-541cc6e5ff87
  00000000-0000-0000-8fe4-092c69639602
  00000000-0000-0000-acfb-6e445e284609
  00000000-0000-0000-96ff-efd5bbcd5afe
  00000000-0000-0000-8d90-2ca5ebac4a51
  Length = 37 rows

Query saving results in a file:

.. code-block:: python

  >>> from astroquery.jwst import JWST
  >>> job = Jwst.launch_job("SELECT TOP 100 \
  >>> instrument_name, observationuri, planeid, calibrationlevel, \
  >>> dataproducttype, targetposition_coordinates_cval1 as target_ra, \
  >>> targetposition_coordinates_cval2 as target_dec \
  >>> FROM jwst.main ORDER BY instrument_name, observationuri", \
  >>> dump_to_file=True)
  >>>
  >>> print(job)

  Jobid: None
  Phase: COMPLETED
  Owner: None
  Output file: sync_20181116164108.xml.gz
  Results: None

  >>> r = job.get_results()
  >>> print(r['solution_id'])

  >>> r = job.get_results()
  >>> print(r['planeid'])

                planeid               
  ------------------------------------
  00000000-0000-0000-9d6d-f192fde74ce4
  00000000-0000-0000-8a85-d34d6a411611
  00000000-0000-0000-969c-a49226673efa
  00000000-0000-0000-8c07-c26c24bec2ee
  00000000-0000-0000-89d2-b42624493c84
  00000000-0000-0000-800d-659917e7bb26
  00000000-0000-0000-8cb6-748fa37d47e3
  00000000-0000-0000-8573-92ad575b8fb4
  00000000-0000-0000-8572-b7b226953a2c
  00000000-0000-0000-8d1d-765c362e3227
                                   ...
  00000000-0000-0000-b7d9-b4686ed37bf0
  00000000-0000-0000-822f-08376ffe6f0b
  00000000-0000-0000-8a8e-8cd48bb4cd7a
  00000000-0000-0000-8a9d-3e1aae1281ba
  00000000-0000-0000-a2ac-1ac288320bf7
  00000000-0000-0000-a20f-835a58ca7872
  00000000-0000-0000-aa9c-541cc6e5ff87
  00000000-0000-0000-8fe4-092c69639602
  00000000-0000-0000-acfb-6e445e284609
  00000000-0000-0000-96ff-efd5bbcd5afe
  00000000-0000-0000-8d90-2ca5ebac4a51
  Length = 37 rows


1.6 Synchronous query on an 'on-the-fly' uploaded table
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A table can be uploaded to the server in order to be used in a query.

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> upload_resource = 'mytable.xml.gz'
  >>> j = Jwst.launch_job(query="SELECT * from tap_upload.table_test", \
  >>>   upload_resource=upload_resource, \
  >>>   upload_table_name="table_test", verbose=True)
  >>> r = j.get_results()
  >>> r.pprint()

  source_id alpha delta
  --------- ----- -----
          a   1.0   2.0
          b   3.0   4.0
          c   5.0   6.0


1.7 Asynchronous query
~~~~~~~~~~~~~~~~~~~~~~

Asynchronous queries save results at server side. These queries can be accessed at any time. For anonymous users, results are kept for three days.

The results can be saved in memory (default) or in a file.

Query without saving results in a file:

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> job = Jwst.launch_job_async("select top 100 * from jwst.main")
  >>> print(job)

  Jobid: 1542383562372I
  Phase: COMPLETED
  Owner: None
  Output file: async_20181116165244.vot
  Results: None

  >>> r = job.get_results()
  >>> print(r['planeid'])

    solution_id
  -------------------
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
                ...
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  1635378410781933568
  Length = 100 rows

Query saving results in a file:

.. code-block:: python

  >>> from astroquery.gaia import Gaia
  >>>
  >>> job = Gaia.launch_job_async("select top 100 * from gaiadr1.gaia_source order by source_id", dump_to_file=True)
  >>>
  >>> print(job)

  Jobid: 1487845273526O
  Phase: COMPLETED
  Owner: None
  Output file: async_20170223112113.vot
  Results: None

  >>> r = job.get_results()
  >>> print(r['solution_id'])

               planeid               
  ------------------------------------
  00000000-0000-0000-9d6d-f192fde74ce4
  00000000-0000-0000-8a85-d34d6a411611
  00000000-0000-0000-969c-a49226673efa
  00000000-0000-0000-8c07-c26c24bec2ee
  00000000-0000-0000-89d2-b42624493c84
  00000000-0000-0000-800d-659917e7bb26
  00000000-0000-0000-8cb6-748fa37d47e3
  00000000-0000-0000-8573-92ad575b8fb4
  00000000-0000-0000-8572-b7b226953a2c
  00000000-0000-0000-8d1d-765c362e3227
                                   ...
  00000000-0000-0000-b7d9-b4686ed37bf0
  00000000-0000-0000-822f-08376ffe6f0b
  00000000-0000-0000-8a8e-8cd48bb4cd7a
  00000000-0000-0000-8a9d-3e1aae1281ba
  00000000-0000-0000-a2ac-1ac288320bf7
  00000000-0000-0000-a20f-835a58ca7872
  00000000-0000-0000-aa9c-541cc6e5ff87
  00000000-0000-0000-8fe4-092c69639602
  00000000-0000-0000-acfb-6e445e284609
  00000000-0000-0000-96ff-efd5bbcd5afe
  00000000-0000-0000-8d90-2ca5ebac4a51
  Length = 37 rows


1.8 Asynchronous job removal
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To remove asynchronous

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> job = Jwst.remove_jobs(["job_id_1","job_id_2",...])


---------------------------
2. Authenticated access
---------------------------

Authenticated users are able to access to TAP+ capabilities (shared tables, persistent jobs, etc.)
In order to authenticate a user, ``login`` or ``login_gui`` methods must be called. After a successful
authentication, the user will be authenticated until ``logout`` method is called.

All previous methods (``query_object``, ``cone_search``, ``load_table``, ``load_tables``, ``launch_job``) explained for
non authenticated users are applicable for authenticated ones.

The main differences are:

* Asynchronous results are kept at server side for ever (until the user decides to remove one of them).
* Users can access to shared tables.


2.1. Login/Logout
~~~~~~~~~~~~~~~~~

Graphic interface


*Note: Tkinter module is required to use login_gui method.*

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> Jwst.login_gui()


Command line


.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> Jwst.login(user='userName', password='userPassword')


It is possible to use a file where the credentials are stored:

*The file must containing user and password in two different lines.*

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> Jwst.login(credentials_file='my_credentials_file')



To perform a logout


.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> Jwst.logout()



2.2. Listing shared tables
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

  >>> from astroquery.jwst import Jwst
  >>> tables = Jwst.load_tables(only_names=True, include_shared_tables=True)
  >>> for table in (tables):
  >>>   print(table.name)

  public.dual
  tap_schema.columns
  tap_schema.key_columns
  tap_schema.keys
  tap_schema.schemas
  tap_schema.tables
  jwst.artifact
  jwst.chunk
  jwst.main
  jwst.observation
  jwst.observationmember
  jwst.part
  jwst.plane
  jwst.plane_inputs
  ...
  user_schema_1.table1
  user_schema_2.table1
  ...


Reference/API
=============

.. automodapi:: astroquery.gaia
    :no-inheritance-diagram:
