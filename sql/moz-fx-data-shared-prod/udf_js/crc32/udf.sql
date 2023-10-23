/*

Calculate the CRC-32 hash of an input string.

The implementation here could be optimized. In particular, it calculates
a lookup table on every invocation which could be cached and reused.
In practice, though, this implementation appears to be fast enough
that further optimization is not yet warranted.

Based on https://stackoverflow.com/a/18639999/1260237
See https://en.wikipedia.org/wiki/Cyclic_redundancy_check

*/
CREATE OR REPLACE FUNCTION udf_js.crc32(data STRING)
RETURNS INT64 DETERMINISTIC
LANGUAGE js
AS
  """
  var makeCRCTable = function(){
    var c;
    var crcTable = [];
    for(var n =0; n < 256; n++){
      c = n;
      for(var k =0; k < 8; k++){
        c = ((c&1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1));
      }
      crcTable[n] = c;
    }
    return crcTable;
  }

  var crc32 = function(str) {
    if (str === null) {
      return null;
    }
    var crcTable = makeCRCTable();
    var crc = 0 ^ (-1);

    for (var i = 0; i < str.length; i++ ) {
      crc = (crc >>> 8) ^ crcTable[(crc ^ str.charCodeAt(i)) & 0xFF];
    }

    return (crc ^ (-1)) >>> 0;
  };

  return crc32(data);
""";

-- Tests
SELECT
  mozfun.assert.equals(308953907, udf_js.crc32("51baf8b4-75d1-3648-b96d-809569b89a12"));
