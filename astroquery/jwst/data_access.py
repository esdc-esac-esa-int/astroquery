# Licensed under a 3-clause BSD style license - see LICENSE.rst
"""
================
JWST Data Access
================

@author: Raul Gutierrez-Sanchez
@contact: raul.gutierrez@sciops.esa.int

European Space Astronomy Centre (ESAC)
European Space Agency (ESA)

Created on 02 nov. 2018


"""

from astropy.utils.data import download_file

__all__ = ['JwstData', 'JwstDataClass']


class JwstDataClass(object):

    """
    JWST Data manager object
    """

    def __init__(self, url=None):
        if url is None:
            self.__ddurl = "http://jwstdev.n1data.lan:8080/server/data?"
        else:
            self.__ddurl = url

    def get_product(self, artifact_id=None):
        """Get a JWST product given its Artifact ID.

        Parameters
        ----------
        artifact_id : str, mandatory
            Artifact ID of the product.

        Returns
        -------
        local_path : str
            Returns the local path that the file was download to.
        """
        
        if artifact_id is None:
            raise ValueError("Missing required argument: 'artifact_id'")
        
        url=self.__ddurl+"RETRIEVAL_TYPE=PRODUCT&DATA_RETRIEVAL_ORIGIN=ASTROQUERY" +\
                    "&ARTIFACTID=" + artifact_id
        
        try:
            file = download_file(url, cache=True )
        except:
            raise ValueError('Product ' + artifact_id + ' not available')
        return file

JwstData = JwstDataClass()
