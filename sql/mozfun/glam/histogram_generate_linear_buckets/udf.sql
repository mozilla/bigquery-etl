-- udf_linear_buckets
CREATE OR REPLACE FUNCTION glam.histogram_generate_linear_buckets(
  min FLOAT64,
  max FLOAT64,
  nBuckets FLOAT64
)
RETURNS ARRAY<FLOAT64> DETERMINISTIC
LANGUAGE js
AS
  '''
  let result = [0];
  for (let i = 1; i < Math.min(nBuckets, max, 10000); i++) {
    let linearRange = (min * (nBuckets - 1 - i) + max * (i - 1)) / (nBuckets - 2);
    result.push(Math.round(linearRange));
  }
  return result;
''';

SELECT
  -- Buckets of CONTENT_FRAME_TIME_VSYNC
  -- https://sql.telemetry.mozilla.org/queries/75803/source
  assert.array_equals(
    [
      --format:off
      0,8,16,24,32,40,48,56,64,72,80,88,96,104,112,120,128,136,144,152,160,168,
      176,184,192,200,208,216,224,232,240,248,256,264,272,280,288,296,304,312,
      320,328,336,344,352,360,368,376,384,392,400,408,416,424,432,440,448,456,
      464,472,480,488,496,504,512,520,528,536,544,552,560,568,576,584,592,600,
      608,616,624,632,640,648,656,664,672,680,688,696,704,712,720,728,736,744,
      752,760,768,776,784,792
      --format:on
    ],
    glam.histogram_generate_linear_buckets(8, 792, 100)
  ),
  -- https://telemetry.mozilla.org/histogram-simulator/#low=1&high=6&n_buckets=4&kind=linear&generate=normal
  assert.array_equals([0, 1, 4, 6], glam.histogram_generate_linear_buckets(1, 6, 4)),
  -- https://telemetry.mozilla.org/histogram-simulator/#low=1&high=20&n_buckets=5&kind=linear&generate=normal
  assert.array_equals([0, 0, 7, 13, 20], glam.histogram_generate_linear_buckets(0, 20, 5))
