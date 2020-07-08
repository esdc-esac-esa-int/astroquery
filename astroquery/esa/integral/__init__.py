"""

@author: Javier Duran
@contact: javier.duran@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 07 July. 2020

"""

from astropy import config as _config


class Conf(_config.ConfigNamespace):
    """
    Configuration parameters for `astroquery.esa.hubble`.
    """
    DATA_ACTION = _config.ConfigItem("https://ila.esac.esa.int/tap/data",
                                     "Main url for retriving integral products")
    TIMEOUT = 60


conf = Conf()

from .core import Integral, IntegralClass

__all__ = ['Integral', 'IntegralClass', 'Conf', 'conf']
