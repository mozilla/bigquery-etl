import ssl
import logging
import geopy.geocoders
from geopy.geocoders import GoogleV3


class MozGeo(object):
    def __init__(self, config):
        self.config = config.MozGeo

        geopy.geocoders.options.default_proxies = config.proxies
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        geopy.geocoders.options.default_ssl_context = ctx
        self.geolocator = GoogleV3(api_key=self.config["google_api_key"])

    def postal_to_coords(self, location):
        logging.debug("postal_to_coords called with %s" % location)
        res = self.geolocator.geocode(components=location)
        if res is None:
            logging.error("No geolocation found for location %s" % location)
            return (None, None)
        else:
            logging.debug(res.raw)
            return (
                res.raw["geometry"]["location"]["lat"],
                res.raw["geometry"]["location"]["lng"],
            )

    def coords_to_timezone(self, coords):
        logging.debug("coords_to_timezone called with %s" % str(coords))
        res = self.geolocator.reverse_timezone(coords)
        logging.debug(res)
        # print(res)
        return res
