/*
Casts a JSON String histogram into a ARRAY<STRUCT<key, value>>.
I found no SQL native way to do this. If you have, please open change or open a ticket.
*/
CREATE OR REPLACE FUNCTION glam.histogram_cast_struct(json_str STRING)
RETURNS ARRAY<STRUCT<KEY STRING, value FLOAT64>>
LANGUAGE js
AS
  """
  if (!json_str) {
    return null
  }
  const json_dict = JSON.parse(json_str);
  const entries = Object.entries(json_dict).map(
      (r)=>Object.fromEntries(
        [["KEY", r[0]],["value", parseFloat(r[1])]]
      )
  );
  return entries;
""";

SELECT
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 0.1111), ("1", 0.6667), ("2", 0)],
    glam.histogram_cast_struct('{"0":0.1111,"1":0.6667,"2":0}')
  ),
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("0", 0.1111), ("1", 0.6667), ("2", 0), ("10", 100)],
    glam.histogram_cast_struct('{"0":0.1111,"1":0.6667,"2":0,"10":100}')
  ),
  assert.array_empty(glam.histogram_cast_struct('{}')),
  assert.array_equals(
    ARRAY<STRUCT<key STRING, value FLOAT64>>[("always", 0.5), ("never", 0.5)],
    glam.histogram_cast_struct('{"always":0.5,"never":0.5}')
  )
