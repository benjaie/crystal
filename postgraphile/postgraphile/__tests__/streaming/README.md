# Testing incremental connection execution

The [connection query](./connection.graphql) selects up to 25 people and their
posts from the PostGraphile v4 kitchen-sink schema. Seven fragments can each be
skipped or deferred. Nodes and edges have independent stream conditions and
initial counts. The test applies patches in arrival order and compares the
assembled data with ordinary execution of the same selection.

From a built checkout with the PostgreSQL test template, run the suite with
`PGHOST` set for your installation.

```sh
yarn workspace postgraphile test:jest --runInBand streaming/connection
```

- The default suite exercises 128 combinations, small and empty pages, pool
  sizes of one and two, cancellation, and the materialization budget.
- CI also runs the 100,001-row export with a zero-row fallback budget. To run
  just that test locally, use the command in its name.

  ```sh
  POSTGRAPHILE_STREAM_LARGE=1 yarn workspace postgraphile test:jest --runInBand streaming/connection -t 100,001
  ```

- Set `POSTGRAPHILE_STREAM_SWEEP=all` when running the suite to exercise all
  65,536 combinations at `first: 2`. Failures report the variables and mask.

The SQL assertions distinguish a page shared by two streamed lists from a page
whose edges are needed upfront. They also check the fetch size, batching of
small sibling pages, and which rows are read when only page metadata is needed.
The [archived forum fixture](../../../../grafast/dataplan-pg/__tests__/queries/conditions/archived-forum-inherited-messages.stream-6.test.graphql)
covers a deferred count independent of the streamed page.
