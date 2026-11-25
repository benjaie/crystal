# Testing incremental connection execution

[The torture query](./connection.graphql) uses the PostGraphile v4 kitchen-sink
schema with simple collections enabled. It selects up to 25 people, a page of
posts per person and an unconditional total count. Each of seven fragments can
be independently skipped or deferred. The node and edge lists have independent
stream conditions and initial counts.

## Running the query

Run the regression cases from a built checkout with the usual PostgreSQL test
template. Set `PGHOST` for your installation.

```sh
cd postgraphile/postgraphile
PGHOST=/var/run/postgresql ../../node_modules/.bin/jest --runInBand \
  --runTestsByPath __tests__/streaming/connection.test.ts
```

The default run covers the large-page export selection, cancellation,
materialization limits, page metadata, and 128 boolean combinations. The fixture
has a one-client pool, two parents with over 1,200 posts each, and short and
empty pages. The test applies patches in arrival order and compares the
assembled data with the same query with its incremental directives removed.

Set `POSTGRAPHILE_STREAM_SWEEP=all` on the same command to exercise all 65,536
boolean combinations at `first: 2`, varying the initial counts. Each combination
has a six-second deadline. A failure reports the variables and mask. Set
`POSTGRAPHILE_STREAM_LARGE=1` to also seed an export of 100,001 rows and compare
it to ordinary execution with a zero-row budget for stream materialization. Use
Jest's `-t '100,001'` to run just that export.

## Regressions found on 6f5d067ad

All 65,536 boolean combinations matched ordinary execution at `first: 2` on this
revision. Larger pages and SQL inspection exposed four problems.

- With every boolean false, `first: 3` and a fallback budget of two rows,
  disabled streams raised a materialization error.
- With `skip2, skip3, skip5, skip6, skip7` true, only the count and previous
  page flag are needed. At `first: 10001`, PostgreSQL built an array of 10,002
  placeholders. Requesting the start cursor fetched 10,002 keys.
- With `stream1, skip3, skip4` true and one parent, enabling `defer1` or
  `defer2` forced an eager fetch before the initial payload.
- All 25-parent cases materialized the child pages, including the export with
  `{"first":100000,"stream1":true,"skip3":true,"skip4":true}`.

## What the SQL checks cover

The export now opens one cursor per parent and fetches at most 100 rows per pull
from PostgreSQL. The tests check that the client can cancel with unread pages
and immediately reuse the only pool connection. Small sibling pages whose
combined upper bound fits a single fetch can use one batched query.

With `{"stream1":true,"skip4":true,"first":100000}`, both lists are selected and
`stream2` defaults to false. The initial payload includes every edge. PostgreSQL
fetches the node and edge columns together in one batched page query, and the
materialized rows supply the later node patches. The request-wide row budget can
therefore be exceeded even when each individual page fits.

Adding `"stream2":true` lets one consumer read each parent's cursor and use each
row for both lists. On an export with 100,056 rows across 25 parents, the 25
cursors returned exactly 100,056 rows for both lists combined. The assembled
response matched ordinary execution with a zero-row materialization budget. To
run the two sibling-list regression cases, add
`-t 'shared page once|both streams enabled'` to the Jest command above.

For forward metadata-only selections, the previous-page flag needs no page rows,
the start cursor needs one, and the next-page flag needs at most the lookahead
row. The end cursor comes from the ordered page itself. Both the ordinary and
incremental forms still compute the requested total count. `explain analyze` on
a page of 10,001 from a parent with 12,209 posts confirmed zero page rows read
for the previous-page flag and one for the start cursor, in addition to the
count scan.

A variable-controlled stream keeps a reusable plan. When both streams are
disabled that plan can issue separate parent/count and batched page queries.
Incompatible stream consumers and upfront end cursors can still require a
materialized page subject to the configured limit.

On `4971f105b`, all 65,536 boolean combinations at 25 parents matched ordinary
execution within the per-case deadline. Another 968 cases varied parent count,
page size, stream prefix and skip/defer settings. The 100,001-row export also
matched with a zero-row fallback budget.

The [archived forum fixture] puts `totalCount` inside the defer alongside
`pageInfo`. The count reads the forum ID independently of the streamed messages.
A planner check previously treated that independence as grounds to materialize
the entire page. The fixture sets the materialization budget to zero and checks
for one PostgreSQL cursor shared by nodes and edges. Run it from the repository
root.

```sh
cd grafast/dataplan-pg
PGHOST=/var/run/postgresql ../../node_modules/.bin/jest --runInBand \
  archived-forum-inherited-messages.stream-6
```

[archived forum fixture]:
  ../../../../grafast/dataplan-pg/__tests__/queries/conditions/archived-forum-inherited-messages.stream-6.test.graphql
