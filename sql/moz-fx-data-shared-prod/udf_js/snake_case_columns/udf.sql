CREATE OR REPLACE FUNCTION udf_js.snake_case_columns(input ARRAY<STRING>)
RETURNS ARRAY<STRING> DETERMINISTIC
LANGUAGE js AS r"""
const REV_WORD_BOUND_PAT = new RegExp(
  "\\b"  // standard word boundary
  + "|(?<=[a-z][A-Z])(?=\\d*[A-Z])"  // A7Aa -> A7|Aa boundary
  + "|(?<=[a-z][A-Z])(?=\\d*[a-z])"  // a7Aa -> a7|Aa boundary
  + "|(?<=[A-Z])(?=\\d*[a-z])"  // a7A -> a7|A boundary
);

/**
 * Convert a name to snake case.
 * 
 * The specific implementation here uses regular expressions in order to be compatible across languages.
 * See https://github.com/acmiyaguchi/test-casing
 */
function format(input) {
  const subbed = input.split('').reverse().join('').replace(/[^\w]|_/g, " ");
  const reversedResult = subbed.split(REV_WORD_BOUND_PAT)
    .map(s => s.trim())
    .map(s => s.toLowerCase())
    .filter(s => s.length > 0)
    .join('_');
  return reversedResult.split('').reverse().join('');
}

/**
 * Convert a name to a BigQuery compatible format.
 *
 * If the name starts with a digit, prepend an underscore.
 * Otherwise, convert the name to snake_case.
 */
function convertNameForBq(name) {
  let sb = '';
  if (name.length > 0 && !isNaN(parseInt(name.charAt(0)))) {
    sb += '_';
  }
  sb += format(name);
  return sb;
}

return input.map((field) => convertNameForBq(field));
""";

-- Tests
WITH input AS (
  SELECT
    ['metrics', 'color'] AS test_input,
    ['metrics', 'color'] AS expected
  UNION ALL
  SELECT
    ['user_prefs', 'foo.bar', 'camelCase'],
    ['user_prefs', 'foo_bar', 'camel_case']
),
formatted AS (
  SELECT
    udf_js.snake_case_columns(test_input) AS result,
    expected
  FROM
    input
)
SELECT
  mozfun.assert.array_equals(expected, result)
FROM
  formatted
