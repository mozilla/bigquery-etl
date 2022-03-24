CREATE OR REPLACE FUNCTION norm.get_country_code(
    str STRING
) AS (
CASE
    {%- for country_code, aliases in code_to_aliases.items() %}
        WHEN LOWER(REGEXP_REPLACE(str, "CTY_", "")) IN ({{ aliases | map('lower') | join(', ') }}) THEN '{{ country_code }}'
    {%- endfor %}
    ELSE str
END
);