SELECT
  id,
  name,
  display_name,
  capabilities,
FROM
  EXTERNAL_QUERY(
    "moz-fx-fxa-nonprod.us.fxa-rds-nonprod-stage-fxa",
    """SELECT
         id,
         name,
         display_name,
         capabilities
       FROM
         fxa.groups
    """
  )