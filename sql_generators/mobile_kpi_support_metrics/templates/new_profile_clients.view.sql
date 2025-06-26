{{ header }}
CREATE OR REPLACE VIEW
  `{{ project_id }}.{{ dataset }}.{{ name }}`
AS
SELECT
  *
FROM
  `{{ project_id }}.{{ dataset }}_derived.{{ name }}_{{ version }}`
