#!/usr/bin/env python3

"""Get experiment list for a given date from the recipe server."""

import json
import time
from argparse import ArgumentParser

import requests

experiments_qualifying_actions = {
    "preference-experiment",
    "opt-out-study",
    "branched-addon-study",
    "multi-preference-experiment",
}
excluded_experiments = {
    None,
    "pref-flip-screenshots-release-1369150",
    "pref-flip-search-composition-57-release-1413565",
    "pref-hotfix-tls-13-avast-rollback",
}
recipes_url = "https://normandy.services.mozilla.com/api/v1/recipe/?format=json"
timestamp_format = "%Y-%m-%dT%H:%M:%S.%f%z"

parser = ArgumentParser(description=__doc__)
parser.add_argument(
    "date",
    type=lambda value: time.strptime(value, "%Y-%m-%d"),
    help="minimum last_updated date for disabled recipes",
)


def get_experiment_list(date):
    for recipe in requests.get(recipes_url).json():
        arguments = recipe.get("arguments") or {}
        branches = arguments.get("branches")
        name = arguments.get("slug", arguments.get("name"))
        last_updated = time.strptime(recipe.get("last_updated"), timestamp_format)
        if (
            "branches" in arguments
            and recipe.get("action") in experiments_qualifying_actions
            and arguments.get("isHighVolume") in {None, False}
            and name not in excluded_experiments
            and (recipe.get("enabled") or last_updated > date)
        ):
            yield name


def main():
    print(json.dumps(list(get_experiment_list(parser.parse_args().date))))


if __name__ == "__main__":
    main()
