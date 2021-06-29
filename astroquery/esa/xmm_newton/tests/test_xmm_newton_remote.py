# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""

@author: Elena Colomo
@contact: ecolomo@esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 4 Sept. 2019
"""

import pytest

import sys
import tarfile
import os
import errno
import shutil
from astropy.coordinates import SkyCoord
from astropy.utils.diff import report_diff_values
from astroquery.utils.tap.core import TapPlus

from ..core import XMMNewtonClass


class TestXMMNewtonRemote():

    @pytest.mark.remote_data
    def test_get_epic_metadata(self):
        tap_url = "http://nxsadev.esac.esa.int/tap-server/tap/"
        target_name = "4XMM J122934.7+015657"
        radius = 0.01
        epic_source_table = "xsa.v_epic_source"
        epic_source_column = "epic_source_equatorial_spoint"
        cat_4xmm_table = "xsa.v_epic_source_cat"
        cat_4xmm_column = "epic_source_cat_equatorial_spoint"
        stack_4xmm_table = "xsa.v_epic_xmm_stack_cat"
        stack_4xmm_column = "epic_stack_cat_equatorial_spoint"
        slew_source_table = "xsa.v_slew_source_cat"
        slew_source_column = "slew_source_cat_equatorial_spoint"
        xsa = XMMNewtonClass(TapPlus(url=tap_url))
        epic_source, cat_4xmm, stack_4xmm, slew_source = xsa.get_epic_metadata(target_name=target_name,
                                                                                radius=radius)
        c = SkyCoord.from_name(target_name, parse=True)
        query = ("select * from {} "
                 "where 1=contains({}, circle('ICRS', {}, {}, {}));")
        table = xsa.query_xsa_tap(query.format(epic_source_table,
                                               epic_source_column,
                                               c.ra.degree,
                                               c.dec.degree,
                                               radius))
        assert report_diff_values(epic_source, table)
        table = xsa.query_xsa_tap(query.format(cat_4xmm_table,
                                               cat_4xmm_column,
                                               c.ra.degree,
                                               c.dec.degree,
                                               radius))
        assert report_diff_values(cat_4xmm, table)
        table = xsa.query_xsa_tap(query.format(stack_4xmm_table,
                                               stack_4xmm_column,
                                               c.ra.degree,
                                               c.dec.degree,
                                               radius))
        assert report_diff_values(stack_4xmm, table)
        table = xsa.query_xsa_tap(query.format(slew_source_table,
                                               slew_source_column,
                                               c.ra.degree,
                                               c.dec.degree,
                                               radius))
        assert report_diff_values(slew_source, table)

    @pytest.mark.remote_data
    def test_get_target_position(self, capsys):
        xsa = XMMNewtonClass()
        ra, dec = xsa.get_target_position('m31')
        assert ra == '41.26875' and dec == '10.68470833'

    @pytest.mark.remote_data
    def test_get_upper_limits(self, capsys):
        xsa = XMMNewtonClass()
        result = xsa.get_upper_limits(ra='41.26875', dec='10.68470833')
        assert result != ''
