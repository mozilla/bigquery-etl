CREATE TEMP FUNCTION udf_js_json_extract_missing_cols (input STRING, indicates_node ARRAY<STRING>, known_nodes ARRAY<STRING>)
RETURNS ARRAY<STRING>
LANGUAGE js AS """
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
        if(typeof next_val === 'object' && next_val !== null && !Array.isArray(next_val)) {
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
            key_paths.push(next_keypath.map(k => '`' + String(k) + '`').join('.'));
        }
    }
    
    return key_paths;
""";

-- Tests

WITH
  addl_properties AS (
    SELECT
      '{"first": {"second": {"third": "hello world"}, "other-second": "value"}}' AS additional_properties ),
    --
  extracted AS (
    SELECT
      udf_js_json_extract_missing_cols(additional_properties, ARRAY[], ARRAY[]) AS no_args,
      udf_js_json_extract_missing_cols(additional_properties, ARRAY['third'], ARRAY[]) AS indicates_node_arg,
      udf_js_json_extract_missing_cols(additional_properties, ARRAY[], ARRAY['first']) AS is_node_arg
    FROM
      addl_properties )
    --
    SELECT
      assert_sets_equals(no_args, ARRAY['`first`.`second`.`third`', '`first`.`other-second`']),
      assert_sets_equals(indicates_node_arg, ARRAY['`first`.`second`', '`first`.`other-second`']),
      assert_sets_equals(no_args, ARRAY['`first`'])
  FROM
    extracted
