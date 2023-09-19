# Stage Deploys 

## Stage Deploys in Continuous Integration

Before changes, such as adding new fields to existing datasets or adding new datasets, can be deployed to production, bigquery-etl's [CI (continuous integration)](https://github.com/mozilla/bigquery-etl/blob/main/.circleci/config.yml) [deploys these changes to a stage environment](https://github.com/mozilla/bigquery-etl/blob/06d7baa3678509abc42ab190c6f1beabc920001c/.circleci/config.yml#L353) and uses these stage artifacts to run its various checks. 

Currently, the `bigquery-etl-integration-test` project serves as the stage environment. CI does have read and write access, but does at no point publish actual data to this project. Only UDFs, table schemas and views are published. The project itself does not have access to any production project, like `mozdata`, so stage artifacts cannot reference any other artifacts that live in production.

Deploying artifacts to stage follows the following steps:
1. Once a new pull-request gets created in bigquery-etl, CI will [pull in the `generated-sql` branch](https://github.com/mozilla/bigquery-etl/blob/06d7baa3678509abc42ab190c6f1beabc920001c/.circleci/config.yml#L367-L372) to [determine all files that show any changes compared to what is deployed in production](https://github.com/mozilla/bigquery-etl/blob/06d7baa3678509abc42ab190c6f1beabc920001c/.circleci/config.yml#L373-L384) (it is assumed that the `generated-sql` branch reflects the artifacts currently deployed in production). All of these changed artifacts (UDFs, tables and views) will be deployed to the stage environment.
    * This CI step runs after the `generate-sql` CI step to ensure that checks will also be executed on generated queries and to ensure `schema.yaml` files have been automatically created for queries. 
2. The `bqetl` CLI has a command to run stage deploys, which is called in the CI: `./bqetl stage deploy --dataset-suffix=$CIRCLE_SHA1 $FILE_PATHS`
    * `--dataset-suffix` will result in the artifacts being deployed to datasets that are suffixed by the current commit hash. This is to prevent any conflicts when deploying changes for the same artifacts in parallel and helps with debugging deployed artifacts.
3. For every artifacts that gets deployed to stage all dependencies need to be determined and deployed to the stage environment as well since the stage environment doesn't have access to production. Before these artifacts get actually deployed, they need to be determined first by traversing artifact definitions.
    * Determining dependencies is only relevant for UDFs and views. For queries, available `schema.yaml` files will simply be deployed. 
    * For UDFs, if a UDF does call another UDF then this UDF needs to be deployed to stage as well.
    * For views, if a view references another view, table or UDF then each of these referenced artifacts needs to be available on stage as well, otherwise the view cannot even be deployed to stage.
    * If artifacts are referenced that are not defined as part of the bigquery-etl repo (like stable or live tables) then their schema will get determined and a placeholder `query.sql` file will be created
    * Also dependencies of dependencies need to be deployed, and so on
4. Once all artifacts that need to be deployed have been determined, all references to these artifacts in existing SQL files need to be updated. These references will need to point to the stage project and the temporary datasets that artifacts will be published to.
    * Artifacts that get deployed are determined from the files that got changed and any artifacts that are referenced in the SQL definitions of these files, as well as their references and so on.
5. To run the deploy, all artifacts will be copied to `sql/bigquery-etl-integration-test` into their corresponding temporary datasets.
    * Also if any existing [SQL tests](https://github.com/mozilla/bigquery-etl/tree/main/tests/sql) the are related to changed artifacts will have their referenced artifacts updated and will get copied to a `bigquery-etl-integration-test` folder
    * The deploy is executed in the order of: UDFs, tables, views
    * UDFs and views get deployed in a way that ensures that the right order of deployments (e.g. dependencies need to be deployed before the views referencing them)
6. Once the deploy has been completed, the CI will use these staged artifacts to run its tests
7. After checks have succeeded, the deployed artifacts [will be removed from stage](https://github.com/mozilla/bigquery-etl/blob/69c0c1d5fca6d7a50455552c30a331751ef2442b/.circleci/config.yml#L577)
    * By default the table expiration is set to 1 hour
    * This step will also automatically remove any tables and datasets that got previously deployed, are older than an hour but haven't been removed (for example due to some CI check failing)

After CI checks have passed and the pull-request has been approved, changes can be merged to `main`. Once a new version of bigquery-etl has been published the changes can be deployed to production through the [`bqetl_artifact_deployment` Airflow DAG](https://workflow.telemetry.mozilla.org/dags/bqetl_artifact_deployment). For more information on artifact deployments to production see: https://docs.telemetry.mozilla.org/concepts/pipeline/artifact_deployment.html

## Local Deploys to Stage

Local changes can be deployed to stage using the `./bqetl stage deploy` command:

```
./bqetl stage deploy \
  --dataset-suffix=test \
  --copy-sql-to-tmp-dir \
  sql/moz-fx-data-shared-prod/firefox_ios/new_profile_activation/view.sql \
  sql/mozfun/map/sum/udf.sql
```

Files (for example ones with changes) that should be deployed to stage need to be specified. The `stage deploy` accepts the following parameters:
* `--dataset-suffix` is an optional suffix that will be added to the datasets deployed to stage
* `--copy-sql-to-tmp-dir` copies SQL stored in `sql/` to a temporary folder. Reference updates and any other modifications required to run the stage deploy will be performed in this temporary directory. This is an optional parameter. If not specified, changes get applied to the files directly and can be reverted, for example, by running `git checkout -- sql/`
* (optional) `--remove-updated-artifacts` removes artifact files that have been deployed from the "prod" folders. This ensures that tests don't run on outdated or undeployed artifacts.

Deployed stage artifacts can be deleted from `bigquery-etl-integration-test` by running:

```
./bqetl stage clean --delete-expired --dataset-suffix=test
```
