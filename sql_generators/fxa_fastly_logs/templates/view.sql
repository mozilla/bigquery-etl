CREATE OR REPLACE VIEW
  `{{ target_project }}.fxa_fastly_logs.{{ env }}_{{ service }}`
AS
SELECT
  *
FROM
  `moz-fx-fxa-{{ env }}.fxa_{{ service }}_{{ env }}_{{ stage }}_fastly_cdn_logs.fastly`
