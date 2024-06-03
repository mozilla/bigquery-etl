import os

config = {
    "proxies": {},
    "MozGeo": {"google_api_key": os.environ.get("MOZGEO_GOOGLE_API_KEY", ""), },
}
