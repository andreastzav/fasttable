# Fast Table

Inspired by [gabrielpetersson/fast-grid](https://github.com/gabrielpetersson/fast-grid), this project is my attempt to push web table performance as far as possible through **single-threaded filtering and sorting** improvements.

## Live demo

[Live demo](https://andreastzav.github.io/fasttable/)
- Load a table preset, e.g. the one with 1 million rows, and play with filtering and sorting. Alternatively you can generate your own table.

## Project direction

The work here intentionally focuses on algorithmic and data-structure improvements on the main thread:

- Faster filtering
- Faster sorting
- Better planning and indexing for both

I did not prioritize building a worker-based filtering/sorting pipeline that splits work into batches and returns partial early results for progressive UI updates. That is a valid product direction, but **it is not** the target of this project.

## Performance results

On a 1,000,000 row table, this project reaches (vs native browser implementations):

- Filtering: about **12 ms** total on average, roughly a **77x** improvement
- Single-column sorting: about **1 ms**, roughly a **1500x** improvement
- Two-column sorting: about **20 ms** on average, roughly a **340x** improvement

With this benchmark profile, I think this may now be the fastest web table implementation.

## Why it is fast

Key ideas used in this codebase include:

- Columnar and binary-columnar filtering paths
- Dictionary key search and dictionary intersection
- Smarter filter planning
- Precomputed sort indices and rank arrays
- Typed comparators and index-based sorting

## Recommended settings

The defaults already selected in the UI are the best-performing choices for this project.

### Filtering defaults

- Use columnar data: on
- Use binary columnar: on
- Use numeric rows: on
- Use normalized strings: on
- Search dict keys: on
- Intersect dicts: on
- Smarter planner: on
- Use smart filtering: off (**on**: reuse the previous filtered subset for stricter follow-up input)
- Use filter cache: off (**on**: store previous filter results in cache for repeated searches)

### Sorting defaults

- Sort mode: use precomp indices

## Notes

Table generation uses web workers by default, but filtering and sorting run on the main thread (single thread).

## Productivity credits

This project was built with the help of **Codex** and **ChatGPT**, which were used as productivity multipliers for faster iteration, implementation support, and documentation.
I think of this workflow as **Lambda coding**:
Human on top providing direction, ideas, constraints, and feedback (plus the occasional curse word); Codex on the right as the hard(ly) working software engineer; and ChatGPT on the left for critique and adversarial feedback.

## TODO

- Make filtering/sorting fully pluggable for other projects (module packaging + public API + UI decoupling).
