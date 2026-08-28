# Optimizer Rewrite Rules — Examples

Optimizations in this package:

- Range Bounds Merge Optimization

Two different mongot query styles supported:

- `$vectorSearch`: `Clause`: `ClauseRewriteRule`: `VectorSearchFilterQueryFactory`
- `$search`: `Operator`: `OperatorRewriteRule`: `CompoundQueryFactory`

**Lucene notation.** Structures are shown in `BooleanQuery.toString()` style, one clause per
line: `+X` MUST, `#X` FILTER, `-X` MUST_NOT, unprefixed `X` SHOULD. Term and point queries are
abbreviated as `field:value` / `field:[lo TO hi]`, and single-clause wrapper `BooleanQuery`
layers that don't affect the point are elided.

---

## Range Bounds Merge Optimization

When the user writes a query with multiple comparison operators (`{$gte: lo}`, `{$lte: hi}`)
for the same field name (the same path), each comparison gets translated into a separate Lucene
point range query, meaning that each comparison results in a separate BKD-tree scan over a
half-open interval that can cover most of the index.

Fusing a comparison pair into one closed range causes Lucene to perform only a single BKD scan
over the intersection `[lo, hi]` which could be much smaller than the entire index.

Three rules implement this optimization, all controlled by `DynamicFeatureFlags.QUERY_OPTIMIZER`
and `DynamicFeatureFlags.RANGE_BOUNDS_MERGE_OPTIMIZATION`:

- `RangeBoundsMergeMqlSimpleClauseRule` (`$vectorSearch`)
- `RangeBoundsMergeMqlAndClauseRule` (`$vectorSearch`)
- `RangeBoundsMergeOperatorRule` (`$search`)

### RangeBoundsMergeMqlSimpleClauseRule ($vectorSearch)

This rule collapses complementary half-open operator pairs on one number/date `SimpleClause` into
closed-range operators, the internal-use-only `ClosedRangeBoundComparisonOperator`. The clause may
carry any number of operators: each lower bound pairs with the earliest unpaired upper bound of the
same value type (number with number, date with date), and any operators outside a fused pair stay
in place. Operators on one `SimpleClause` are independent conjuncts, so fusing two of them cannot
affect the rest.

```js
{
    $vectorSearch: { ...,
        filter: {
            year: {$gte: 1990, $lte: 2000}
        }
    }
}
```

Unoptimized would mean each MQL operator translates to its own half-open BKD scan:

```
+year:[1990 TO *]
+year:[* TO 2000]
```

Optimized — one closed-range BKD scan, often covering a much smaller range:

```
+year:[1990 TO 2000]
```

Extra operators on the clause do not block the merge. For example:

```js
{
    $vectorSearch: { ...,
        filter: {
            year: {
                $gte: 1990,
                $lte: 2000,
                $ne: 1999
            }
        }
    }
}
```

fuses the `$gte`/`$lte` pair and leaves the `$ne` untouched:

```
+year:[1990 TO 2000]
+(the $ne translation, unchanged)
```

A pair whose fused form would be contradictory (lower endpoint above upper, or equal endpoints
with an exclusive side) is left unfused. Pairing is greedy in operator order, without
backtracking: a contradictory pair takes both of its members out of consideration even if another
pairing could have fused.

### RangeBoundsMergeMqlAndClauseRule ($vectorSearch)

MQL also supports specifying the same field name more than once. This rule normalizes two names into
one, which can then in turn allow the `RangeBoundsMergeMqlSimpleClauseRule` rule to fire:

```js
{
    $vectorSearch: { ...,
        filter: {
            $and: [{year: {$gte: 1990}}, {year: {$lte: 2000}}]
        }
    }
}
```

is rewritten to (the `$and` wrapper survives; the rule only merges its children):

```js
{
    $vectorSearch: { ...,
        filter: {
            $and: [{year: {$gte: 1990, $lte: 2000}}]
        }
    }
}
```

On its own this changes no Lucene structure — two Lucene `MUST` range clauses either way.

### RangeBoundsMergeOperatorRule ($search)

This rule is the same as `RangeBoundsMergeMqlSimpleClauseRule` but for the `$search` operator tree.

#### 1. The main optimization

```js
{
    $search: {
        compound: {
            filter: [
                {range: {path: "year", gte: 1990}},
                {range: {path: "year", lte: 2000}}
            ]
        }
    }
}
```

Unoptimized would give two independent half-open BKD scans:

```
#year:[1990 TO *]
#year:[* TO 2000]
```

Optimized — one fully-bounded scan over the intersection:

```
#year:[1990 TO 2000]
```

#### 2. Array data falls back at translation

Range semantics on arrays are per-element: `{gte: 1990}` matches via `2005`, `{lte: 2000}`
matches via `1985`, so the document matches both half-open ranges — but no single element falls
inside `[1990, 2000]`, so the fused single-scan form would drop it.

```js
// year is multivalued: some document contains  { year: [1985, 2005] }
{
    $search: {
        compound: {
            filter: [
                {range: {path: "year", gte: 1990}},
                {range: {path: "year", lte: 2000}}
            ]
        }
    }
}
```

The rule fuses this pair anyway — it never consults the index. The fused operator carries a
`fusedByOptimizer` marker, and translation performs the array-data check with the query's
embedded root in hand: when any relevant point field is multivalued (or the check cannot read
the index), the marked operator translates as a conjunction of its two half-open ranges instead
of one closed scan.

Unoptimized — two sibling half-open filter clauses:

```
#year:[1990 TO *]
#year:[* TO 2000]
```

Optimized with array data present — the same two scans, nested inside the fused clause:

```
#(+year:[1990 TO *] +year:[* TO 2000])
```

A user-written closed range (`{range: {path: "year", gte: 1990, lte: 2000}}`) carries no marker
and is never decomposed: its per-element array semantics are documented behavior, so it always
translates as a single closed-range query.

#### 3. The optimization does not apply to multi-path inputs

```js
{
    $search: {
        compound: {
            filter: [
                {range: {path: ["year", "founded"], gte: 1990}},
                {range: {path: ["year", "founded"], lte: 2000}}
            ]
        }
    }
}
```

A multi-path range matches when *any one* of its paths is in range, so `founded` may satisfy the
first operator and `year` the second. The fused form would require one path to satisfy both
bounds, dropping matches. Only pairs with the same single path fuse; this pair is left unchanged:

```
#(year:[1990 TO *] founded:[1990 TO *])
#(year:[* TO 2000] founded:[* TO 2000])
```

#### 4. The optimization does not apply to `must` inputs:

```js
{
    $search: {
        compound: {
            must: [
                {range: {path: "year", gte: 1990}},
                {range: {path: "year", lte: 2000}}
            ]
        }
    }
}
```

Each `must` clause contributes its own score, so fusing a `must` pair (two scored clauses → one)
would change user-visible scoring. Only the score-free `filter` slot is rewritten; this query is
left unchanged:

```
+year:[1990 TO *]
+year:[* TO 2000]
```

#### 5. The optimization does not apply to contradictory inputs

```js
{
    $search: {
        compound: {
            filter: [
                {range: {path: "year", gte: 2000}},
                {range: {path: "year", lte: 1990}}
            ]
        }
    }
}
```

#### 6. `doesNotAffect` must match

```js
{
    $search: {
        compound: {
            filter: [
                {range: {path: "year", gte: 1990}},
                {range: {path: "year", lte: 2000, doesNotAffect: ["yearFacet"]}}
            ]
        }
    }
}
```

`doesNotAffect` excludes its operator from counting the named facets, per operator. A single
fused operator cannot express excluding only one half of the pair, so a pair fuses only when
both operators carry the same `doesNotAffect` fields (order-insensitive, absent equivalent to
empty). This pair is left unchanged.

