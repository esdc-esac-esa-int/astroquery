# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
=============
JWST TAP plus
=============

@author: Raul Gutierrez-Sanchez
@contact: raul.gutierrez@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 23 oct. 2018


"""

from astroquery.utils.tap.core import TapPlus
from astropy import config as _config


class Conf(_config.ConfigNamespace):
    """
    Configuration parameters for `astroquery.jwst`.
    """

    JWST_OBSERVATION_TABLE = _config.ConfigItem("jwst.observation",
                                         "JWST observation table")
    JWST_OBSERVATION_TABLE_RA = _config.ConfigItem("targetposition_coordinates_cval1",
                                            "Name of RA parameter in table")
    JWST_OBSERVATION_TABLE_DEC = _config.ConfigItem("targetposition_coordinates_cval2",
                                             "Name of Dec parameter in table")


conf = Conf()

gaia = TapPlus(url="http://jwstdev.esac.esa.int/server/tap", verbose=False)

from .core import Jwst, JwstClass

__all__ = ['Jwst', 'JwstClass', 'Conf', 'conf']
