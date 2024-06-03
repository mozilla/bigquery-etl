import logging
import re

from .secrets_util import config as util_config


class LocalConfig(object):
    def __init__(self):
        pass

    def __getattr__(self, attr):
        return util_config[attr]


_config = LocalConfig()


def set_up_logging(level):
    log_level = logging.INFO
    if re.match("^debug$", level, flags=re.IGNORECASE):
        log_level = logging.DEBUG
    elif re.match("^info$", level, flags=re.IGNORECASE):
        log_level = logging.INFO
    elif re.match("^warn", level, flags=re.IGNORECASE):
        log_level = logging.WARNING
    elif re.match("^err", level, flags=re.IGNORECASE):
        log_level = logging.ERROR
    elif re.match("^crit", level, flags=re.IGNORECASE):
        log_level = logging.CRITICAL
    logging.basicConfig(
        format="[%(asctime)s] %(name)s [%(levelname)s]: %(message)s", level=log_level
    )


def postal_to_coords_and_timezone(loc):
    from .classes.mozgeo import MozGeo

    geo = MozGeo(_config)
    coords = geo.postal_to_coords(loc)
    if coords != (None, None):
        tz = geo.coords_to_timezone(coords)
    else:
        tz = None
    return (coords, tz)
