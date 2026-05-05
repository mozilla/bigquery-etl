/*

Extract missing columns from additional properties.

More generally, get a list of nodes from a JSON blob. Array elements are indicated as [...].

param input: The JSON blob to explode
param indicates_node: An array of strings. If a key's value is an object, and contains one of these values,
                      that key is returned as a node.
param known_nodes: An array of strings. If a key is in this array, it is returned as a node.

Notes:
- Use indicates_node for things like histograms. For example ['histogram_type'] will ensure that each
  histogram will be returned as a missing node, rather than the subvalues within the histogram
  (e.g. values, sum, etc.)
- Use known_nodes if you're aware of a missing section, like ['simpleMeasurements']

See here for an example usage
https://sql.telemetry.mozilla.org/queries/64460/source

*/
CREATE OR REPLACE FUNCTION udf_js.json_extract_missing_cols(
  input STRING,
  indicates_node ARRAY<STRING>,
  known_nodes ARRAY<STRING>
)
RETURNS ARRAY<STRING> DETERMINISTIC
LANGUAGE js
AS
  """
    if (input == null || input == '{}') {
      return null;
    }

    var key_paths = [];
    var parsed = JSON.parse(input);
    var remaining_paths = Object.keys(parsed).map(k => [k]);

    while(remaining_paths && remaining_paths.length){
        var next_keypath = remaining_paths.pop();
        var next_val = next_keypath.reduce((obj, k) => obj[k], parsed);

        var is_node = true;
        if(typeof next_val === 'object' && next_val !== null) {
            is_node = false;

            var keys = Object.keys(next_val);

            if(known_nodes.indexOf(next_keypath[next_keypath.length - 1]) > -1){
                is_node = true;
            }

            var keys_indicating_node = keys.filter(
                e => indicates_node.indexOf(e) > -1
            )

            if(keys_indicating_node.length > 0){
                is_node = true;
            }

            if(!is_node){
                Object.keys(next_val).map(k =>
                    remaining_paths.push(next_keypath.concat([k]))
                );
            }
        }

        if(is_node) {
            key_path_str = next_keypath.map(k => '`' + String(k) + '`').join('.');
            array_index_regex = /\`[0-9]+\`/gi;
            key_path_str = key_path_str.replace(array_index_regex, "[...]")
            key_paths.push(key_path_str);
        }
    }

    unique_key_paths = key_paths.filter((v, i, a) => a.indexOf(v) === i);
    return unique_key_paths;
""";

-- Tests
WITH addl_properties AS (
  SELECT
    '''
      {
        "first": {
          "second": {
            "third": "hello world"
          },
          "other-second": "value",
          "array": [
            {
              "duplicate-array-element": "value",
              "unique-array-element": "value"
            },
            {
              "duplicate-array-element": "value",
              "nested-array-element": {"nested": "value"}
            }
          ]
        }
      }
      ''' AS additional_properties
),
    --
extracted AS (
  SELECT
    udf_js.json_extract_missing_cols(additional_properties, ARRAY[], ARRAY[]) AS no_args,
    udf_js.json_extract_missing_cols(
      additional_properties,
      ARRAY['third'],
      ARRAY[]
    ) AS indicates_node_arg,
    udf_js.json_extract_missing_cols(additional_properties, ARRAY[], ARRAY['first']) AS is_node_arg
  FROM
    addl_properties
)
    --
SELECT
  mozfun.assert.array_equals_any_order(
    no_args,
    ARRAY[
      '`first`.`second`.`third`',
      '`first`.`other-second`',
      '`first`.`array`.[...].`nested-array-element`.`nested`',
      '`first`.`array`.[...].`duplicate-array-element`',
      '`first`.`array`.[...].`unique-array-element`'
    ]
  ),
  mozfun.assert.array_equals_any_order(
    indicates_node_arg,
    ARRAY[
      '`first`.`second`',
      '`first`.`other-second`',
      '`first`.`array`.[...].`nested-array-element`.`nested`',
      '`first`.`array`.[...].`duplicate-array-element`',
      '`first`.`array`.[...].`unique-array-element`'
    ]
  ),
  mozfun.assert.array_equals_any_order(is_node_arg, ARRAY['`first`'])
FROM
  extracted
