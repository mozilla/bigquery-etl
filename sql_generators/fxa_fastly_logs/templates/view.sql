CREATE OR REPLACE VIEW
  `{{ target_project }}.{{ dataset }}.{{ env }}_{{ service }}`
AS
SELECT
  *
FROM
  `moz-fx-fxa-{{ env }}.fxa_{{ service }}_{{ env }}_{{ stage }}_fastly_cdn_logs.fastly`
