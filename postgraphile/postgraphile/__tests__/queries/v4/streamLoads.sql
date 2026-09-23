select
  __person__."id"::text as "0"
from "c"."person" as __person__
order by __person__."id" asc
limit 1;

select
  __post__."id"::text as "0",
  __post__."headline" as "1"
from "a"."post" as __post__
where (
  __post__."author_id" = $1::"int4"
)
order by __post__."id" asc;