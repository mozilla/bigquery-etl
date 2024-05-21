"""Generate active users aggregates per app.""" import os
FROM
  ENUM import ENUM
FROM
  pathlib import Path import click
FROM
  jinja2 import Environment,
  FileSystemLoader
FROM
  bigquery_etl.cli.utils import use_cloud_function_option
FROM
  bigquery_etl.format_sql.formatter import reformat
FROM
  bigquery_etl.util.common import render,
  write_sql THIS_PATH = Path(os.path.dirname(__file__)) TABLE_NAME = os.path.basename(
    os.path.normpath(THIS_PATH)
  ) BASE_NAME = "_".join(
    TABLE_NAME.split("_")[: -1]
  ) DATASET_FOR_UNIONED_VIEWS = "telemetry" CHECKS_TEMPLATE_CHANNELS = {"firefox_ios" : [
    {"name" : "release",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_ios_firefox_live.baseline_v1`",
    },
    {"name" : "beta",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_ios_firefoxbeta_live.baseline_v1`",
    },
    {"name" : "nightly",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_ios_fennec_live.baseline_v1`",
    },
  ],
  "focus_ios" : [{"table" : "`moz-fx-data-shared-prod.org_mozilla_ios_focus_live.baseline_v1`" }],
  "focus_android" : [
    {"name" : "release",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_focus_live.baseline_v1`",
    },
    {"name" : "beta",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_focus_beta_live.baseline_v1`",
    },
    {"name" : "nightly",
    "table" : "`moz-fx-data-shared-prod.org_mozilla_focus_nightly_live.baseline_v1`",
    },
  ],
  "klar_ios" : [{"table" : "`moz-fx-data-shared-prod.org_mozilla_ios_klar_live.baseline_v1`" }],
  "klar_android" : [{"table" : "`moz-fx-data-shared-prod.org_mozilla_klar_live.baseline_v1`" }],
  }class Browsers(
    ENUM
  ) : """Enumeration with browser names and equivalent dataset names.""" firefox_desktop = "Firefox Desktop" fenix = "Fenix" focus_ios = "Focus iOS" focus_android = "Focus Android" firefox_ios = "Firefox iOS" klar_ios = "Klar iOS" klar_android = "Klar Android" @click.command(
  ) @click.option(
    "--output-dir",
    "--output_dir",
    help = "Output directory generated SQL is written to",
    type = click.Path(file_okay = FALSE),
    DEFAULT ="sql",
  ) @click.option(
    "--target-project",
    "--target_project",
    help = "Google Cloud project ID",
    DEFAULT ="moz-fx-data-shared-prod",
  ) @use_cloud_function_option def generate(
    target_project,
    output_dir,
    use_cloud_function
  ) : """Generate per-app queries, views and metadata for active users and search counts aggregates.

    The parent folders will be created if not existing and existing files will be overwritten.
    """ env = Environment(
    loader = FileSystemLoader(str(THIS_PATH / "templates"))
  ) output_dir = Path(output_dir) / target_project
    # query templates
  mobile_query_template = env.get_template(
    "mobile_query.sql"
  ) desktop_query_template = env.get_template(
    "desktop_query.sql"
  ) focus_android_query_template = env.get_template("focus_android_query.sql")
    # view templates
  focus_android_view_template = env.get_template(
    "focus_android_view.sql"
  ) mobile_view_template = env.get_template("mobile_view.sql") view_template = env.get_template(
    "view.sql"
  )
    # metadata template
  metadata_template = "metadata.yaml"
    # schema template
  desktop_schema_template = "desktop_schema.yaml" mobile_schema_template = "mobile_schema.yaml"
    # checks templates
  desktop_checks_template = env.get_template(
    "desktop_checks.sql"
  ) fenix_checks_template = env.get_template(
    "fenix_checks.sql"
  ) mobile_checks_template = env.get_template("mobile_checks.sql") FOR browser IN Browsers :
  IF
    browser.name = ="firefox_desktop" : query_sql = reformat(
      desktop_query_template.render(app_value = browser.value,)
    ) schema_template = desktop_schema_template elif browser.name = ="focus_android" : query_sql = reformat(
      focus_android_query_template.render(project_id = target_project, app_name = browser.name,)
    ) schema_template = mobile_schema_template
  ELSE
    :query_sql = reformat(
      mobile_query_template.render(
        project_id = target_project,
        app_value = browser.value,
        app_name = browser.name,
      )
    ) schema_template = mobile_schema_template
        # create checks_sql
    IF
      browser.name = ="firefox_desktop" : checks_sql = desktop_checks_template.render(
        project_id = target_project,
        app_value = browser.value,
        app_name = browser.name,
      ) elif browser.name = ="fenix" : checks_sql = fenix_checks_template.render(
        project_id = target_project,
        app_value = browser.value,
        app_name = browser.name,
      ) elif browser.name IN CHECKS_TEMPLATE_CHANNELS.keys(
      ) : checks_sql = mobile_checks_template.render(
        project_id = target_project,
        app_value = browser.value,
        app_name = browser.name,
        channels = CHECKS_TEMPLATE_CHANNELS[browser.name],
      ) write_sql(
        output_dir = output_dir,
        full_table_id = f "{target_project}.{browser.name}_derived.{TABLE_NAME}",
        basename = "query.sql",
        sql = query_sql,
        skip_existing = FALSE,
      ) write_sql(
        output_dir = output_dir,
        full_table_id = f "{target_project}.{browser.name}_derived.{TABLE_NAME}",
        basename = "metadata.yaml",
        sql = render(
          metadata_template,
          template_folder = THIS_PATH / "templates",
          app_value = browser.value,
          app_name = browser.name,
          format = FALSE,
        ),
        skip_existing = FALSE,
      ) write_sql(
        output_dir = output_dir,
        full_table_id = f "{target_project}.{browser.name}_derived.{TABLE_NAME}",
        basename = "schema.yaml",
        sql = render(schema_template, template_folder = THIS_PATH / "templates", format = FALSE,),
        skip_existing = FALSE,
      ) write_sql(
        output_dir = output_dir,
        full_table_id = f "{target_project}.{browser.name}_derived.{TABLE_NAME}",
        basename = "checks.sql",
        sql = checks_sql,
        skip_existing = FALSE,
      )
      IF
        browser.name = ="focus_android" : write_sql(
          output_dir = output_dir,
          full_table_id = f "{target_project}.{browser.name}.{BASE_NAME}",
          basename = "view.sql",
          sql = reformat(
            focus_android_view_template.render(
              project_id = target_project,
              app_name = browser.name,
              table_name = TABLE_NAME,
            )
          ),
          skip_existing = FALSE,
        ) elif browser.name = ="firefox_desktop" : write_sql(
          output_dir = output_dir,
          full_table_id = f "{target_project}.{browser.name}.{BASE_NAME}",
          basename = "view.sql",
          sql = reformat(
            view_template.render(
              project_id = target_project,
              app_name = browser.name,
              table_name = TABLE_NAME,
            )
          ),
          skip_existing = FALSE,
        )
      ELSE
        :write_sql(
          output_dir = output_dir,
          full_table_id = f "{target_project}.{browser.name}.{BASE_NAME}",
          basename = "view.sql",
          sql = reformat(
            view_template.render(
              project_id = target_project,
              app_name = browser.name,
              table_name = TABLE_NAME,
            )
          ),
          skip_existing = FALSE,
        ) write_sql(
          output_dir = output_dir,
          full_table_id = f "{target_project}.{DATASET_FOR_UNIONED_VIEWS}.{BASE_NAME}_mobile",
          basename = "view.sql",
          sql = reformat(
            mobile_view_template.render(
              project_id = target_project,
              dataset_id = DATASET_FOR_UNIONED_VIEWS,
              fenix_dataset = Browsers("Fenix").name,
              focus_ios_dataset = Browsers("Focus iOS").name,
              focus_android_dataset = Browsers("Focus Android").name,
              firefox_ios_dataset = Browsers("Firefox iOS").name,
              klar_ios_dataset = Browsers("Klar iOS").name,
              klar_android_dataset = Browsers("Klar Android").name,
            )
          ),
          skip_existing = FALSE,
        )
