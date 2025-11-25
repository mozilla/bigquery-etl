# Terms of Use Generator

Generator responsible for generating artifacts related to terms of use queries for the purpose of streamlining analysis related to Terms of Use. Each app can be configured individually to use specific template.

---

The following templates exist:

- terms_of_use_status_v1 - query.sql, metadata.yaml, schema.yaml, bigconfig.yml, and view.sql

*If you wish to generate a user facing view, please include view.sql.jinja in the template's folder.*

---

## Configuration

bqetl_project.yaml config file is used to determine for which applications and specific channels the generation artifacts should be created for. This app and channel list is specified under `generate`, `terms_of_use` configuration. Here's an example of a valid configuration for this generator:

```yaml
generate:
  [...]
  terms_of_use:
    bigeye_defaults:
      collection: Operational Checks
      notification_channel: '#de-bigeye-triage'
    apps:
      fenix:
        templates:
        - terms_of_use_status_v1
        bigeye:  # optional, bigeye_defaults will be used if not specified
          collection: Operational Checks  # optional
          notification_channel: '#de-bigeye-triage'  # optional
      firefox_ios:
        templates:
        - terms_of_use_status_v1
        bigeye:  # optional
          collection: Operational Checks  # optional
          notification_channel: '#de-bigeye-triage'  # optional
      firefox_desktop:
        templates:
        - terms_of_use_status_v1
        bigeye:  # optional
          collection: Operational Checks  # optional
          notification_channel: '#de-bigeye-triage'  # optional
```

## Running the generator

The following command can be used in the roor directory of the project to run the generator:

```bash
./bqetl generate terms_of_use
```

Additional options exist, those can be found using the following command:

```bash
./bqetl generate terms_of_use --help
```