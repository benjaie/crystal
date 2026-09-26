select
  __single_table_items_2."id"::text as "0",
  __single_table_items_2."type"::text as "1",
  array(
    select array[
      __single_table_items__."id"::text
    ]::text[]
    from "polymorphic"."single_table_items" as __single_table_items__
    where (
      __single_table_items__."parent_id" = __single_table_items_2."id"
    )
    order by __single_table_items__."id" asc
  )::text as "2"
from "polymorphic"."single_table_items" as __single_table_items_2
order by __single_table_items_2."id" asc;

with __single_table_items_identifiers__ as materialized (
  select ids.ordinality - 1 as idx, (ids.value->>0)::"int4" as "id0" from json_array_elements($1::json) with ordinality as ids
)
select __single_table_items_result__.*
from __single_table_items_identifiers__,
lateral (
  select
    __single_table_items__."type"::text as "0",
    __single_table_items__."id"::text as "1",
    __single_table_items__."position"::text as "2",
    __single_table_items_identifiers__.idx as "3"
  from "polymorphic"."single_table_items" as __single_table_items__
  where (
    __single_table_items__."id" = __single_table_items_identifiers__."id0"
  )
) as __single_table_items_result__;