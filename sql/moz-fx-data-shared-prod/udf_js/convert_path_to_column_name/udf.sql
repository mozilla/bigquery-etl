/*
Converts the `path` column in `monitoring.telemetry_missing_columns` to a format in 'INFORMATION_SCHEMA'. 
This is to check if the missing column has been added to the table

See:
- https://bugzilla.mozilla.org/show_bug.cgi?id=1823724
for more context

*/

CREATE OR REPLACE FUNCTION udf_js.format_path_string(input STRING)
RETURNS STRING
LANGUAGE js
AS r"""
    probe_name = input.split('`').map((el, i) => i % 2 ? '`' + el + '`' : el).filter((el) => el != '.');
    result = [];
    var key_path_str ='';
    for (var i = 0; i < probe_name.length; i++) {
            if (i == 3){
            key_path_str = probe_name[i].replaceAll('.','_');
            }
            else{
            key_path_str = probe_name[i].replace('.[...]','');
            }
            if (key_path_str != ''){
            result.push(key_path_str);
            }
            
    };
    return [result];
""";

--Tests
SELECT assert.equals("`metrics`.`timing_distribution`.`fog.ipc.flush_duratinns`.`values`.[...]",  udf_js.format_path_string("`metrics`,`timing_distribution`,`fog_ipc_flush_duratinns`,`values`")),
assert.null(null, udf.format_path_string(null)),
assert.equals("`processStartTimestamp`",udf_js.format_path_string("`processStartTimestamp`")),
assert.equals("`environment`,`settings`,`intl`,`acceptLanguaces`",udf.format_path_string("`environment`.`settings`.`intl`.`acceptLanguaces`.[...]"))
