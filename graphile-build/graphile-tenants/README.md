# @graphile/tenants

This preset hides database-managed tenant columns from automatically generated
GraphQL fields and inputs. It shares `schema.pgMultiTenancy` configuration and
foreign-key classification with `@graphile/simplify-inflection`:

```ts
const preset = {
  extends: [TenantPreset],
  schema: {
    pgMultiTenancy: [
      { table: "gyms", columns: ["gym_id"], implicit: [true] },
      {
        table: "families",
        columns: ["gym_id", "family_id"],
        implicit: [true, false],
      },
    ],
  },
};
```

`implicit` defaults to `false`. Only columns marked `true` are intended to be
hidden; scope relationships and columns are recognized from PostgreSQL foreign
keys, not names alone. This preset does not add tenant values to plans or
queries, and does not create defaults, triggers, or row-level security (RLS)
policies.

Only mark a value implicit when PostgreSQL supplies it and independently
enforces tenant isolation for reads, inserts, updates, and deletes. In
particular, omitting the tenant argument from a scoped unique lookup is safe
only if the database execution context (normally an RLS policy using
transaction-local tenant context) limits visible rows to that tenant. A default
value alone does not provide lookup isolation. If `(gym_id, slug)` is unique,
that does not make `slug` globally unique; the database must ensure a lookup or
mutation by `slug` can affect at most one row in the active context.

The initial release supports only database-managed `true` values. It preserves
existing Node ID semantics: tables with single-column primary keys retain their
existing IDs; composite Node IDs continue to encode their primary-key values,
including implicit columns, and are not made tenant-independent.
