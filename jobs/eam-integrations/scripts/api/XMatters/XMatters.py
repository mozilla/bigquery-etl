import json
import logging
import re

import requests

# from integrations.api.connectors import Util
from api.util import Util
# from integrations.api.connectors.XMatters.secrets_xmatters import config as xm_config
from api.XMatters.secrets_xmatters import config as xm_config

logger = logging.getLogger(__name__)

USE_BASIC_AUTH = True


class LocalConfig(object):
    def __init__(self):
        new_api_suffix = "/api/xm/1"
        old_api_suffix = "/reapi/2015-04-01/"
        self.debug = 3
        self.base_URL = xm_config["url"] + new_api_suffix
        self.base_URL_old_api = xm_config["url"] + old_api_suffix

        self.access_token = False

    def __getattr__(self, attr):
        return xm_config[attr]


_config = LocalConfig()


def get_access_token():
    if not _config.access_token:
        _config.access_token = _get_access_token()
    return _config.access_token


def _get_access_token():
    endpoint_URL = "/oauth2/token"
    grant_type = "password"
    url = (
        _config.base_URL
        + endpoint_URL
        + "?grant_type="
        + grant_type
        + "&client_id="
        + _config.xm_client_id
        + "&username="
        + _config.xm_username
        + "&password="
        + _config.xm_password
    )

    if USE_BASIC_AUTH:
        return ""

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, headers=headers, proxies=_config.proxies)

    if response.status_code == 200:
        rjson = response.json()
        access_token = rjson.get("access_token")
    else:
        print(response.status_code)
        logger.critical(response.json())
        error = "Could not get an access token"
        logger.critical(error)
        raise Exception(error)

    return access_token


# get all xmatters sites
# OLD API
# https://help.xmatters.com/OnDemand/xmodwelcome/communicationplanbuilder/appendixrestapi.htm?cshid=apiGETsites#GETsites
#
def get_all_sites():
    """Gets all sites from xMatters

  Parameters:
    None

  Returns:
    dict: site name -> xmatters site id
      for all sites with status "ACTIVE"

  Example:
    {
      [...]
      'US Remote (NV)' : '656173de-809d-46c3-82f3-dd718efa6af4',
      'Israel Remote'  : '7ed98c24-c73f-4fa5-b987-a005e80e2d63',
      'US Remote (NM)' : '5224215b-337f-4c6b-bc6a-6211246e2086',
      'US Remote (LA)' : '96c2de74-247d-4bb8-b0ec-592b86768f85',
      [...]
    }
  """
    # https://mozilla-np.xmatters.com/xmatters.com/reapi/2015-04-01/sites
    logger.info("\n")
    logger.info("Gathering all XMatters sites")
    all_sites_url = _config.base_URL_old_api + "sites"
    xm_sites = {}
    xm_sites_inactive = {}
    while True:
        response = requests.get(
            all_sites_url,
            auth=(_config.xm_username, _config.xm_password),
            proxies=_config.proxies,
        )
        if response.status_code == 200:
            rjson = response.json()
            logger.debug(rjson)
        else:
            error = "Could not get sites"
            logger.critical(error)
            raise Exception(error)

        for site in rjson["sites"]:
            logger.debug(site["name"] + " -- " + site["identifier"])
            logger.debug(site)
            if site["status"] == "ACTIVE":
                xm_sites[site["name"]] = site["identifier"]
            else:
                xm_sites_inactive[site["name"]] = site["identifier"]

        if rjson["nextRecordsURL"] == "":
            logger.debug("No nextRecordsURL found. done with fetching")
            break
        else:
            logger.debug("NEXT RECORDS URL FOUND: %s" % rjson["nextRecordsURL"])
            all_sites_url = _config.url + rjson["nextRecordsURL"]

    return xm_sites, xm_sites_inactive


# get all people from xmatters
# NEW API
# https://help.xmatters.com/xmAPI/?python#get-people
#
def get_all_people():
    """Gets all users/people from xMatters

  Parameters:
    None

  Returns:
    dict: name -> { attributes }

  Example:
    {
      [...]
      'test@mozilla.com' : {
        u'recipientType': u'PERSON',
        u'status': u'ACTIVE',
        u'firstName': u'Chris',
        u'lastName': u'Test',
        u'links': {u'self': u'/api/xm/1/people/fc97c634-6e9b-4788-bdca-xxxxxxxxxxxx'},
        u'externallyOwned': False,
        u'site': {
          u'id': u'2e6f8d1c-ced7-460e-bf5a-902109021090',
          u'links':
           { u'self': u'/api/xm/1/sites/2e6f8d1c-ced7-460e-bf5a-902109021090' },
          u'name': u'Beverly Hills Office'
        },
        u'properties': {
          u'Functional Group': u'IT',
          u'Executive': False,
          u'Manager Email': u'bbixby@mozilla.com',
          u'Home Country': u'United States of America',
          u'Cost Center': u'1440 - Enterprise Applications and Services (EApps)',
          u'Manager': u'Bill Bixby',
          u'Home Zipcode': u'90210',
          u'MERT/EVAC/Warden': False,
          u'Home City': u'Beverly Hills'
        },
        u'language': u'en',
        u'webLogin': u'test@mozilla.com',
        u'timezone': u'US/Pacific',
        u'targetName': u'test@mozilla.com',
        u'id': u'fc97c634-6e9b-4788-bdca-xxxxxxxxxxxx'
      },
      [...]
    }
  """
# https://mozilla-np.xmatters.com
# https://mozilla-np.xmatters.com/api/xm/
    logger.info("\n")
    logger.info("Gathering all XMatters people")
    url = _config.base_URL + "/people"

    headers = {"Authorization": "Bearer " + get_access_token()}
    if USE_BASIC_AUTH:
        headers = {}

    xm_people = {}
    while True:
        if USE_BASIC_AUTH:
            response = requests.get(
                url,
                auth=(_config.xm_username, _config.xm_password),
                proxies=_config.proxies,
            )
        else:
            response = requests.get(url, headers=headers, proxies=_config.proxies)

        if response.status_code == 200:
            rjson = response.json()
            logger.debug(
                "Retrieved "
                + str(rjson["count"])
                + " of "
                + str(rjson["total"])
                + " people."
            )
        else:
            logger.critical(response)
            raise Exception(response.content)

        for person in rjson["data"]:
            logger.debug(
                "%s %s (%s)"
                % (person["firstName"], person["lastName"], person["targetName"])
            )
            if person["lastName"] in ["[NO LAST NAME]", "NO LAST NAME"]:
                person["lastName"] = ""
            if person["firstName"] in ["[NO FIRST NAME]", "NO FIRST NAME"]:
                person["firstName"] = ""
            xm_people[person["targetName"]] = person

        # Only to backfill data
        #      devs = get_devices_by_person(person['id'])
        #      if not devs:
        #        add_work_email_device(person)

        if "next" in rjson["links"]:
            url = _config.url + rjson["links"]["next"]
        else:
            break

    return xm_people


def get_devices_by_person(person_id):
    """Gets a person's device(s)

  Parameters:
    ID (targetName or id)

  Returns:
    {
      "count":3,
      "total":3,
      "data":[
      {
        "id":"a4d69579-f436-4e85-9d93-703714d85d72",
        "name":"Home Phone",
        "recipientType":"DEVICE",
        "phoneNumber":"+13235553643",
        "targetName":"akaur",
        "deviceType":"VOICE",
    [...]
  """

    logger.info("\n")
    logger.info("Gathering all devices for %s" % person_id)
    url = _config.base_URL + "/people/" + person_id + "/devices"

    headers = {"Authorization": "Bearer " + get_access_token()}
    if USE_BASIC_AUTH:
        headers = {}

    xm_devices = []
    while True:
        if USE_BASIC_AUTH:
            response = requests.get(
                url,
                auth=(_config.xm_username, _config.xm_password),
                proxies=_config.proxies,
            )
        else:
            response = requests.get(url, headers=headers, proxies=_config.proxies)

        if response.status_code == 200:
            rjson = response.json()
            logger.debug(
                "Retrieved "
                + str(rjson["count"])
                + " of "
                + str(rjson["total"])
                + " devices."
            )
        else:
            logger.critical(response)
            raise Exception(response.content)

        for device in rjson["data"]:
            logger.debug("Device - %s %s" % (device["name"], device["targetName"]))
            xm_devices.append(device)

        if "next" in rjson["links"]:
            url = _config.url + rjson["links"]["next"]
        else:
            break

    return xm_devices


def add_work_email_device(xm_user):
    logger.info("Adding device %s to XMatters" % (xm_user["targetName"]))
    url = _config.base_URL + "/devices"

    if USE_BASIC_AUTH:
        headers = {"Content-Type": "application/json"}
    else:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + get_access_token(),
        }

    if not re.search("@", xm_user["targetName"]):
        logger.error(
            "NOT adding device for %s because that ain't no email address!"
            % xm_user["targetName"]
        )
        return False

    device_data = {
        "recipientType": "DEVICE",
        "deviceType": "EMAIL",
        "owner": xm_user["id"],
        "name": "Work Email",
        "emailAddress": xm_user["targetName"],
        "delay": 0,
        "priorityThreshold": "MEDIUM",
        "testStatus": "UNTESTED",
    }

    if USE_BASIC_AUTH:
        response = requests.post(
            url,
            headers=headers,
            auth=(_config.xm_username, _config.xm_password),
            data=json.dumps(device_data),
            proxies=_config.proxies,
        )
    else:
        response = requests.post(
            url, headers=headers, data=json.dumps(device_data), proxies=_config.proxies
        )

    if response.status_code == 201:
        return True
    else:
        logger.critical(
            "ERROR: something went wrong adding device for user %s"
            % (xm_user["targetName"])
        )
        logger.critical(response)
        logger.critical(response.content)
        raise Exception(response.content)


# add_task site to xmatters
# OLD API
# https://help.xmatters.com/OnDemand/xmodwelcome/communicationplanbuilder/appendixrestapi.htm?cshid=apiGETsites#GETsites
#
def add_site(site):
    logger.info("Adding site %s to XMatters" % site)

    # TODO: move this to a config file or something?
    # fixup "bad" data
    if site["country"] == "United States of America":
        site["country"] = "United States"
    elif site["country"] == "Vietnam":
        site["country"] = "Viet Nam"
    elif site["country"] == "Czechia":
        site["country"] = "Czech Republic"
    elif site["country"] == "Tanzania":
        site["country"] = "Tanzania, United Republic of"
    elif site["country"] == "Korea, Republic of":
        site["country"] = "South Korea"
    if site["postal_code"] == "CZECH REPUBLIC":
        site["postal_code"] = ""

    if len(site["postal_code"]) > 10:
        site["postal_code"] = site["postal_code"][:10]

    if "timezone" not in site or "latitude" not in site or "longitude" not in site:
        (coords, tz) = Util.postal_to_coords_and_timezone(
            {"country": site["country"], "postal_code": site["postal_code"]}
        )
        lat = coords[0]
        lng = coords[1]
        if tz is None:
            tz = "America/Los_Angeles"  # arbitrary
        if lat is None:
            lat = 0
        if lng is None:
            lng = 0

    site_data = {
        "name": site["name"],
        "timezone": str(tz),
        # skip address for now as it's mostly bad data (remote sites)
        # 'address1':  site['address'],
        "country": site["country"],
        "city": site["city"],
        # 'state':     site['state'],
        "postalCode": site["postal_code"],
        "latitude": lat,
        "longitude": lng,
        "language": "English",
        "status": "ACTIVE",
    }
    sites_url = _config.base_URL_old_api + "sites"

    headers = {"Content-Type": "application/json"}

    response = requests.post(
        sites_url,
        auth=(_config.xm_username, _config.xm_password),
        headers=headers,
        data=json.dumps(site_data),
        proxies=_config.proxies,
    )
    if response.status_code == 200 or response.status_code == 201:
        rjson = response.json()
        logger.debug(rjson)
        return site_data  # for the unittests
    else:
        logger.critical("Could not create site")
        logger.critical(response.content)
        raise Exception(response.content)


def set_site_inactive(xm_site_id):
    return set_site_status(xm_site_id, "INACTIVE")


def set_site_active(xm_site_id):
    return set_site_status(xm_site_id, "ACTIVE")


# OLD API
# https://help.xmatters.com/OnDemand/xmodwelcome/communicationplanbuilder/appendixrestapi.htm?cshid=apiGETsites#GETsites
#
def set_site_status(xm_site_id, status):
    logger.info("Setting site %s to %s" % (xm_site_id, status))

    site_data = {
        "status": status,
    }
    sites_url = _config.base_URL_old_api + "sites/" + xm_site_id

    headers = {"Content-Type": "application/json"}

    response = requests.post(
        sites_url,
        auth=(_config.xm_username, _config.xm_password),
        headers=headers,
        data=json.dumps(site_data),
        proxies=_config.proxies,
    )
    if response.status_code == 200:
        rjson = response.json()
        logger.debug(rjson)
        return True
    else:
        logger.critical("Could not deactivate site")
        logger.critical(response.content)
        raise Exception(response.content)


def add_new_sites(wd_sites, xm_sites, xm_sites_inactive):
    logger.info("Adding new sites to XMatters")
    xm_sites_in_wd = {}
    for wd_site in wd_sites:
        if wd_site in xm_sites:
            logger.debug("WD site %s found in XMatters! No action." % wd_site)
            xm_sites_in_wd[wd_site] = 1
        elif wd_site in xm_sites_inactive:
            logger.info("WD site %s INACTIVE in XMatters! Reactivating." % wd_site)
            set_site_active(xm_sites_inactive[wd_site])
        else:
            logger.info(
                "WD site %s NOT found in XMatters! Adding to XMatters." % wd_site
            )
            add_site(wd_sites[wd_site])

    return xm_sites_in_wd


def delete_sites(xm_sites, xm_sites_in_wd):
    logger.info("\n")
    logger.info("Deleting empty sites from XMatters")
    for site in xm_sites:
        if site not in xm_sites_in_wd and site != "Mountain View Office":
            logger.info(
                "Site %s not in WorkDay. INACTIVATING %s from XMatters"
                % (site, xm_sites[site])
            )
            set_site_inactive(xm_sites[site])

    return True


def sanitize_string_properties(input_str: str) -> str:
    """Remove known unwanted string patterns from string properties."""
    known_patterns = [
        "[C]",  # Used to identify contractors in Workday
        "[c]",
    ]
    for pattern in known_patterns:
        input_str = input_str.replace(pattern, "")

    # remove possible whitespace from previous step
    input_str = input_str.strip()

    # keep only chars expected in international names
    return re.sub(r"[^A-Za-z0-9_À-ÿ ,.'-]", "", input_str, flags=re.U)

# NEW API
# https://help.xmatters.com/xmAPI/?python#modify-a-person
#


def update_user(wd_user, xm_user, xm_sites):

    logger.info(
        "Updating user %s (%s) in XMatters" % (xm_user["id"], xm_user["targetName"])
    )
    url = _config.base_URL + "/people"

    if USE_BASIC_AUTH:
        headers = {"Content-Type": "application/json"}
    else:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + get_access_token(),
        }

    manager_name = ""
    if "Worker_s_Manager" in wd_user:
        manager_name = (
            wd_user["Worker_s_Manager"][0]["User_Manager_Preferred_First_Name"]
            + " "
            + wd_user["Worker_s_Manager"][0]["User_Manager_Preferred_Last_Name"]
        )
    site_key = (
        wd_user.get("User_Home_Country", "")
        + ":"
        + wd_user.get("User_Home_Postal_Code", "")
    )
    person_data = {
        "id": xm_user["id"],
        "firstName": sanitize_string_properties(
            wd_user.get("User_Preferred_First_Name", "[NO FIRST NAME]")
        ),
        "lastName": sanitize_string_properties(
            wd_user.get("User_Preferred_Last_Name", "[NO LAST NAME]")
        ),
        "site": xm_sites[site_key],
        "properties": {
            "Cost Center": wd_user.get("User_Cost_Center", ""),
            "Manager": manager_name,
            "Manager Email": wd_user.get("User_Manager_Email_Address", ""),
            "Functional Group": wd_user.get("User_Functional_Group", ""),
            "Home City": wd_user.get("User_Home_City", ""),
            "Home Country": wd_user.get("User_Home_Country", ""),
            "Home Zipcode": wd_user.get("User_Home_Postal_Code", "")[:20],
            "Work Location": wd_user.get("User_Work_Location", ""),
        },
    }

    logger.debug("will upload this:")
    logger.debug(json.dumps(person_data))

    if USE_BASIC_AUTH:
        response = requests.post(
            url,
            headers=headers,
            auth=(_config.xm_username, _config.xm_password),
            data=json.dumps(person_data),
            proxies=_config.proxies,
        )
    else:
        response = requests.post(
            url, headers=headers, data=json.dumps(person_data), proxies=_config.proxies
        )

    if response.status_code == 200:
        return person_data  # for the unittest
    else:
        logger.critical(
            "ERROR: something went wrong updating user %s (%s)"
            % (xm_user["id"], xm_user["targetName"])
        )
        logger.critical(response)
        raise Exception(response.content)
# NEW API
# https://help.xmatters.com/xmAPI/?python#create-a-person
#


def add_user(wd_user, xm_sites):
    logger.info("Adding user %s to XMatters" % (wd_user["User_Email_Address"]))
    url = _config.base_URL + "/people"

    if USE_BASIC_AUTH:
        headers = {"Content-Type": "application/json"}
    else:
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + get_access_token(),
        }

    manager_name = ""
    if "Worker_s_Manager" in wd_user:
        manager_name = (
            wd_user["Worker_s_Manager"][0]["User_Manager_Preferred_First_Name"]
            + " "
            + wd_user["Worker_s_Manager"][0]["User_Manager_Preferred_Last_Name"]
        )
    site_key = (
        wd_user.get("User_Home_Country", "")
        + ":"
        + wd_user.get("User_Home_Postal_Code", "")
    )
    person_data = {
        "firstName": sanitize_string_properties(
            wd_user.get("User_Preferred_First_Name", "[NO FIRST NAME]")
        ),
        "lastName": sanitize_string_properties(
            wd_user.get("User_Preferred_Last_Name", "[NO LAST NAME]")
        ),
        "targetName": wd_user["User_Email_Address"],
        "site": xm_sites[site_key],
        "recipientType": "PERSON",
        "status": "ACTIVE",
        "roles": ["Standard User"],
        "supervisors": [_config.supervisor_id],
        "properties": {
            "Cost Center": wd_user.get("User_Cost_Center", ""),
            "Manager": manager_name,
            "Manager Email": wd_user.get("User_Manager_Email_Address", ""),
            "Functional Group": wd_user.get("User_Functional_Group", ""),
            "Home City": wd_user.get("User_Home_City", ""),
            "Home Country": wd_user.get("User_Home_Country", ""),
            "Home Zipcode": wd_user.get("User_Home_Postal_Code", "")[:20],
            "Work Location": wd_user.get("User_Work_Location", ""),
        },
    }

    logger.debug("will upload this:")
    logger.debug(json.dumps(person_data))

    if USE_BASIC_AUTH:
        response = requests.post(
            url,
            headers=headers,
            auth=(_config.xm_username, _config.xm_password),
            data=json.dumps(person_data),
            proxies=_config.proxies,
        )
    else:
        response = requests.post(
            url, headers=headers, data=json.dumps(person_data), proxies=_config.proxies
        )

    if response.status_code == 201:
        rjson = response.json()
    else:
        logger.critical(
            "ERROR: something went wrong adding user %s"
            % (wd_user["User_Email_Address"])
        )
        logger.critical(response)
        logger.critical(response.content)
        raise Exception(response.content)

    person_data["id"] = rjson["id"]
    add_work_email_device(person_data)

    return person_data  # for the unittest


# NEW API
# https://help.xmatters.com/xmAPI/?python#delete-a-person
#
def actual_person_delete(target):
    logger.info("Sending DELETE request for %s" % target)

    url = _config.base_URL + "/people/" + target

    headers = {"Authorization": "Bearer " + get_access_token()}

    if USE_BASIC_AUTH:
        response = requests.delete(
            url,
            auth=(_config.xm_username, _config.xm_password),
            proxies=_config.proxies,
        )
    else:
        response = requests.delete(url, headers=headers, proxies=_config.proxies)

    if response.status_code == 200:
        logger.info("Deleted person " + response.json().get("targetName"))
        return True
    elif response.status_code == 204:
        logger.warning("The person could not be found.")
        return False
    else:
        logger.critical("Could not delete person!")
        logger.critical(response)
        logger.critical(response.content)
        raise Exception(response.content)


def delete_users(xm_users, users_seen_in_wd):
    logger.info("\n")
    logger.info("Deleting old users from XMatters")
    for user in xm_users:
        if not re.search("@", user):
            # let's just skip any usernames that don't look like emails
            continue
        if user not in users_seen_in_wd:
            logger.info("User %s not seen in workday, will delete from xmatters" % user)
            actual_person_delete(user)

    return True
