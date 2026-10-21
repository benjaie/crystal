create table gyms (
  id uuid primary key,
  name text not null
);

create table families (
  id uuid not null,
  gym_id uuid not null references gyms (id),
  name text not null,
  primary key (id),
  unique (gym_id, id),
  unique (gym_id, name)
);

create table subscriptions (
  id uuid primary key,
  gym_id uuid not null,
  family_id uuid not null,
  description text not null,
  unique (gym_id, family_id, id),
  foreign key (gym_id) references gyms (id),
  foreign key (gym_id, family_id) references families (gym_id, id)
);
comment on table subscriptions is E'@name gym_subscription';

create table allocations (
  id uuid primary key,
  gym_id uuid not null,
  family_id uuid not null,
  subscription_id uuid not null,
  description text not null,
  unique (gym_id, family_id, id),
  foreign key (gym_id) references gyms (id),
  foreign key (gym_id, family_id) references families (gym_id, id),
  foreign key (family_id, gym_id, subscription_id)
    references subscriptions (family_id, gym_id, id)
);

create table memberships (
  id uuid primary key,
  gym_id uuid not null,
  family_id uuid not null,
  name text not null,
  unique (gym_id, family_id, id),
  foreign key (gym_id) references gyms (id),
  foreign key (gym_id, family_id) references families (gym_id, id)
);

create table membership_allocations (
  id uuid primary key,
  gym_id uuid not null,
  family_id uuid not null,
  membership_id uuid not null,
  allocation_id uuid not null,
  unique (gym_id, family_id, membership_id, allocation_id),
  foreign key (gym_id) references gyms (id),
  foreign key (gym_id, family_id) references families (gym_id, id),
  foreign key (gym_id, family_id, membership_id)
    references memberships (gym_id, family_id, id),
  foreign key (gym_id, family_id, allocation_id)
    references allocations (gym_id, family_id, id)
);

create table gym_links (
  id uuid primary key,
  target_gym_id uuid references gyms (id)
);

create table product_variants (
  id uuid primary key,
  product_id uuid not null,
  variant_id uuid not null,
  unique (product_id, variant_id)
);

create table variant_references (
  id uuid primary key,
  product_id uuid not null,
  variant_id uuid not null,
  foreign key (product_id, variant_id)
    references product_variants (product_id, variant_id)
);
