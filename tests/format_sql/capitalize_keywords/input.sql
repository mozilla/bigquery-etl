-- keywords
* except;
* replace;
all;
and;
any;
array;
as;
asc;
assert_rows_modified;
at;
between;
by;
cast;
collate;
contains;
create;
cross;
cube;
current;
current row;
day;
dayofweek;
dayofyear;
default;
define;
desc;
distinct;
enum;
escape;
except;
exclude;
exists;
extract;
false;
fetch;
following;
for;
for system_time as of;
from;
full;
group;
grouping;
groups;
hash;
having;
hour;
if();
ignore;
in;
inner;
inout;
intersect;
interval;
into;
is;
isoweek;
isoyear;
join;
lateral;
left;
like;
limit;
lookup;
merge;
microsecond;
millisecond;
minute;
month;
natural;
new;
no;
not;
null;
nulls;
nulls first;
nulls last;
of;
on;
or;
order;
out;
outer;
over;
partition;
preceding;
proto;
quarter;
range;
recursive;
replace;
respect;
right;
rollup;
rows;
second;
select;
set;
some;
struct;
tablesample;
to;
treat;
true;
unbounded;
union;
unnest;
using;
week;
where;
window;
with;
within;
year;
-- newline keywords
pivot;
unpivot;
unpivot include nulls;
unpivot exclude nulls;
with offset;
create or replace function;
create or replace aggregate function;
create or replace table function;
create function if not exists;
create aggregate function if not exists;
create table function if not exists;
create function;
create temp function;
create aggregate function;
create temp aggregate function;
create table function;
returns;
language;
and;
between;
or;
xor;
declare;
raise;
raise using message;
-- scripting keywords
create or replace procedure
begin
end;
create procedure if not exists
begin
end;
create procedure
begin
exception when error then
end;
if
then
elseif
then
else
end if;
while
do
end while;
loop
end loop;
repeat
until
end repeat;
for _ in ()
do
end for;
case
when
then
else
end;
begin transaction;
break;
call;
commit transaction;
commit;
continue;
execute immediate '...' into result using parameter;
iterate;
leave;
return;
rollback transaction;
rollback;
-- top level keywords
alter table if exists;
alter table;
cluster by;
create or replace table;
create or replace view;
create or replace materialized view;
create table if not exists;
create view if not exists;
create materialized view if not exists;
create temp table;
create table;
create view;
create materialized view;
drop table;
drop view;
options;
delete from;
delete;
insert into;
insert;
merge into;
merge;
update;
cross join;
except distinct;
from;
full join;
full outer join;
group by;
group by all;
group by cube ();
group by grouping sets ();
group by rollup ();
having;
inner join;
intersect distinct;
join;
left join;
left outer join;
limit;
order by;
partition by;
right join;
right outer join;
rows between;
rows;
select;
select all;
select distinct;
select as struct;
select as value;
union all;
union distinct;
values;
when matched;
when not matched;
when not matched by source;
when not matched by target;
where;
with;
window;
-- operator keywords
not between;
not in;
is distinct from;
is not distinct from;
is null;
is not null;
is true;
is false;
is not true;
is not false;
like any;
like some;
like all;
not like;
not like any;
not like some;
not like all;
-- data type keywords
bignumeric;
bignumeric(10, 2);
bigdecimal;
bigdecimal(10, 2);
bool;
boolean;
bytes;
bytes(100);
date;
datetime;
float64;
geography;
int64;
int;
smallint;
integer;
bigint;
tinyint;
byteint;
json;
numeric;
numeric(10, 2);
decimal;
decimal(10, 2);
range<date>;
range<datetime>;
range<timestamp>;
string;
string(100);
time;
timestamp;
