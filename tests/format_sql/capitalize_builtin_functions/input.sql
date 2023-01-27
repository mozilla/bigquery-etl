-- Built-in functions
abs();

acos();

acosh();

aead.decrypt_bytes();

aead.decrypt_string();

aead.encrypt();

any_value();

approx_count_distinct();

approx_quantiles();

approx_top_count();

approx_top_sum();

array();

array_agg();

array_concat();

array_concat_agg();

array_length();

array_reverse();

array_to_string();

ascii();

asin();

asinh();

atan();

atan2();

atanh();

avg();

bit_and();

bit_count();

bit_or();

bit_xor();

bool();

byte_length();

cast();

cbrt();

ceil();

ceiling();

char_length();

character_length();

chr();

coalesce();

code_points_to_bytes();

code_points_to_string();

collate();

concat();

contains_substr();

corr();

cos();

cosh();

cot();

coth();

count();

countif();

covar_pop();

covar_samp();

csc();

csch();

cume_dist();

current_date();

current_datetime();

current_time();

current_timestamp();

date();

date_add();

date_diff();

date_from_unix_date();

date_sub();

date_trunc();

datetime();

datetime_add();

datetime_diff();

datetime_sub();

datetime_trunc();

dense_rank();

deterministic_decrypt_bytes();

deterministic_decrypt_string();

deterministic_encrypt();

div();

ends_with();

error();

exp();

external_object_transform();

extract();

farm_fingerprint();

first_value();

float64();

floor();

format();

format_date();

format_datetime();

format_time();

format_timestamp();

from_base32();

from_base64();

from_hex();

generate_array();

generate_date_array();

generate_timestamp_array();

generate_uuid();

greatest();

hll_count.extract();

hll_count.init();

hll_count.merge();

hll_count.merge_partial();

ieee_divide();

if();

ifnull();

initcap();

instr();

int64();

is_inf();

is_nan();

json_extract();

json_extract_array();

json_extract_scalar();

json_extract_string_array();

json_query();

json_query_array();

json_type();

json_value();

json_value_array();

justify_days();

justify_hours();

justify_interval();

keys.add_key_from_raw_bytes();

keys.keyset_chain();

keys.keyset_from_json();

keys.keyset_length();

keys.keyset_to_json();

keys.new_keyset();

keys.new_wrapped_keyset();

keys.rewrap_keyset();

keys.rotate_keyset();

keys.rotate_wrapped_keyset();

lag();

last_day();

last_value();

lead();

least();

left();

length();

ln();

log();

log10();

logical_and();

logical_or();

lower();

lpad();

ltrim();

make_interval();

max();

md5();

min();

mod();

net.host();

net.ip_from_string();

net.ip_net_mask();

net.ip_to_string();

net.ip_trunc();

net.ipv4_from_int64();

net.ipv4_to_int64();

net.public_suffix();

net.reg_domain();

net.safe_ip_from_string();

normalize();

normalize_and_casefold();

nth_value();

ntile();

nullif();

octet_length();

offset();

ordinal();

parse_bignumeric();

parse_date();

parse_datetime();

parse_json();

parse_numeric();

parse_time();

parse_timestamp();

percent_rank();

percentile_cont();

percentile_disc();

pow();

power();

rand();

range_bucket();

rank();

regexp_contains();

regexp_extract();

regexp_extract_all();

regexp_instr();

regexp_replace();

regexp_substr();

repeat();

replace();

reverse();

right();

round();

row_number();

rpad();

rtrim();

s2_cellidfrompoint();

s2_coveringcellids();

safe_add();

safe_cast();

safe_convert_bytes_to_string();

safe_divide();

safe_multiply();

safe_negate();

safe_offset();

safe_ordinal();

safe_subtract();

sec();

sech();

session_user();

sha1();

sha256();

sha512();

sign();

sin();

sinh();

soundex();

split();

sqrt();

st_angle();

st_area();

st_asbinary();

st_asgeojson();

st_astext();

st_azimuth();

st_boundary();

st_boundingbox();

st_buffer();

st_bufferwithtolerance();

st_centroid();

st_centroid_agg();

st_closestpoint();

st_clusterdbscan();

st_contains();

st_convexhull();

st_coveredby();

st_covers();

st_difference();

st_dimension();

st_disjoint();

st_distance();

st_dump();

st_dwithin();

st_endpoint();

st_equals();

st_extent();

st_exteriorring();

st_geogfrom();

st_geogfromgeojson();

st_geogfromtext();

st_geogfromwkb();

st_geogpoint();

st_geogpointfromgeohash();

st_geohash();

st_geometrytype();

st_interiorrings();

st_intersection();

st_intersects();

st_intersectsbox();

st_isclosed();

st_iscollection();

st_isempty();

st_isring();

st_length();

st_makeline();

st_makepolygon();

st_makepolygonoriented();

st_maxdistance();

st_npoints();

st_numgeometries();

st_numpoints();

st_perimeter();

st_pointn();

st_simplify();

st_snaptogrid();

st_startpoint();

st_touches();

st_union();

st_union_agg();

st_within();

st_x();

st_y();

starts_with();

stddev();

stddev_pop();

stddev_samp();

string();

string_agg();

strpos();

substr();

substring();

sum();

tan();

tanh();

time();

time_add();

time_diff();

time_sub();

time_trunc();

timestamp();

timestamp_add();

timestamp_diff();

timestamp_micros();

timestamp_millis();

timestamp_seconds();

timestamp_sub();

timestamp_trunc();

to_base32();

to_base64();

to_code_points();

to_hex();

to_json();

to_json_string();

translate();

trim();

trunc();

unicode();

unix_date();

unix_micros();

unix_millis();

unix_seconds();

upper();

var_pop();

var_samp();

variance();

-- SAFE. prefix
safe.substr('foo', 0, -2);

-- UDFs with same name as built-in function left as is
mozfun.map.sum();
