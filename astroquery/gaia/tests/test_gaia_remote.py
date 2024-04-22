import astropy.units as u
import pytest
from astropy.coordinates import SkyCoord

from .. import GaiaClass


@pytest.mark.remote_data
def test_query_object_columns_with_radius():
    # Regression test: `columns` were ignored if `radius` was provided [#2025]
    gaia = GaiaClass()
    sc = SkyCoord(ra=0 * u.deg, dec=0 * u.deg)
    table = gaia.query_object_async(sc, radius=10 * u.arcsec, columns=['ra'])
    assert table.colnames == ['ra', 'dist']


@pytest.mark.remote_data
def test_query_object_row_limit():
    gaia = GaiaClass()
    coord = SkyCoord(ra=280, dec=-60, unit=(u.degree, u.degree), frame='icrs')
    width = u.Quantity(0.1, u.deg)
    height = u.Quantity(0.1, u.deg)
    r = gaia.query_object_async(coordinate=coord, width=width, height=height)

    assert len(r) == gaia.ROW_LIMIT

    gaia.ROW_LIMIT = 10
    r = gaia.query_object_async(coordinate=coord, width=width, height=height)

    assert len(r) == 10 == gaia.ROW_LIMIT

    gaia.ROW_LIMIT = -1
    r = gaia.query_object_async(coordinate=coord, width=width, height=height)

    assert len(r) == 184


@pytest.mark.remote_data
def test_cone_search_row_limit():
    gaia = GaiaClass()
    coord = SkyCoord(ra=280, dec=-60, unit=(u.degree, u.degree), frame='icrs')
    radius = u.Quantity(0.1, u.deg)
    j = gaia.cone_search_async(coord, radius=radius)
    r = j.get_results()

    assert len(r) == gaia.ROW_LIMIT

    gaia.ROW_LIMIT = 10
    j = gaia.cone_search_async(coord, radius=radius)
    r = j.get_results()

    assert len(r) == 10 == gaia.ROW_LIMIT

    gaia.ROW_LIMIT = -1
    j = gaia.cone_search_async(coord, radius=radius)
    r = j.get_results()

    assert len(r) == 1218

@pytest.mark.remote_data
def test_query_async_object_columns_with_epoch_prop():
    # Regression test: `columns` were ignored if `radius` was provided [#2025]
    gaia = GaiaClass()
    radius = u.Quantity(0.001, u.deg)
    table = gaia.query_object_async('Proxima Centauri', radius=radius)
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    table = gaia.query_object_async('Proxima Centauri', radius=radius, epoch_prop=True)
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'


@pytest.mark.remote_data
def test_query_sync_object_columns_with_epoch_prop():
    # Regression test: `columns` were ignored if `radius` was provided [#2025]
    gaia = GaiaClass()
    radius = u.Quantity(0.001, u.deg)
    table = gaia.query_object('Proxima Centauri', radius=radius)
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    table = gaia.query_object('Proxima Centauri', radius=radius, epoch_prop=True)
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'

def test_cone_search_async_epoch_prop():
    gaia = GaiaClass()
    gaia.ROW_LIMIT = -1

    #ICRS coord. (ep=J2000) : 14:29:42.9461331854 -62:40:46.164680672
    coord = SkyCoord(ra='14:29:42.9461331854', dec='-62:40:46.164680672', unit=(u.hourangle, u.deg), frame='icrs')
    radius = u.Quantity(0.001, u.deg)

    job = gaia.cone_search_async(coord, radius=radius)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    job = gaia.cone_search_async(coord, radius=radius, epoch_prop=True)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'

    job = gaia.cone_search_async('Proxima Centauri', radius=radius)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    job = gaia.cone_search_async('Proxima Centauri', radius=radius, epoch_prop=True)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'

def test_cone_search_epoch_prop():
    gaia = GaiaClass()
    gaia.ROW_LIMIT = -1

    #ICRS coord. (ep=J2000) : 14:29:42.9461331854 -62:40:46.164680672
    coord = SkyCoord(ra='14:29:42.9461331854', dec='-62:40:46.164680672', unit=(u.hourangle, u.deg), frame='icrs')
    radius = u.Quantity(0.001, u.deg)

    job = gaia.cone_search(coord, radius=radius)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    job = gaia.cone_search(coord, radius=radius, epoch_prop=True)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'

    job = gaia.cone_search('Proxima Centauri', radius=radius)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498678802473344'

    job = gaia.cone_search('Proxima Centauri', radius=radius, epoch_prop=True)
    table = job.get_results()
    assert len(table) == 4
    assert table['source_id'][0] == '5853498713160606720'

