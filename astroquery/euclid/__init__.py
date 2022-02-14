# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
===============
Euclid TAP plus
===============
@author: Juan Carlos Segovia (original code from Gaia) / John Hoar (Euclid adaptation)
@contact: juan.carlos.segovia@sciops.esa.int
European Space Astronomy Centre (ESAC)
European Space Agency (ESA)
Created on 30 jun. 2016
Euclid adaptation: 15 March 2018
"""

from astroquery.utils.tap.core import TapPlus
from astropy import config as _config


class Conf(_config.ConfigNamespace):
    """
    Configuration parameters for `astroquery.euclid`.
    """

    urlBase = _config.ConfigItem(
        'https://eas.esac.esa.int/tap-server/tap',
        'Euclid base URL')
    ROW_LIMIT = _config.ConfigItem(50,
                                   "Number of rows to return from database "
                                   "query (set to -1 for unlimited).")


conf = Conf()

""""euclid = TapPlus(url="https://eas.esac.esa.int/tap-dev/tap", verbose=False)"""

from .core import Euclid, EuclidClass

__all__ = ['Euclid', 'EuclidClass', 'Conf', 'conf']