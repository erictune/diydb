# DIY Database
`diydb` is a toy Database modeled after Sqlite, written in Rust as a learning exercise.

# Goals
For the author to:
- learn Rust
- re-learn b-trees
- explore basic database optimizations

# Non-goals
- compatibility with SQL or Sqlite
- making something a useful to others


# Current State

- Can open some very simple sqlite database files and dump the contents.
- No writing yet.  Inputs are created using `sqlite3` CLI.
- very basic demand paging.


# Future Work
In no particular order.
- Data
  - Make Record and Btree iterators
  - Support multi-page btrees.
  - Support indexes.
  - Support blobs 
  - Support overflow
- Concurrency
  - Locking Database file when accessing.
  - Pager layer to support multiple accessors with overlapping lifetimes.
- Write support
  - inserts
    - insert (limited to single page btree per table.)
    - insert (with btree growth and rebalancing.)
    - insert (blob overflow page.)
  - deletes/modifys with size change
    - delete table (would need freelist, vacuum/compaction)
    - delete or modify row
      - needs btree rebalance 
      - needs page defrag and freeblock support.
  - persistence
    - write state to disk at exit
    - write state after single-page update completed.
    - write multi-page in crash-safe way (e.g. with journal or WAL) 
  - concurrency
    - transactions
    - locking pages
- Parsing
  - first milestone will be parsing non-joined, non-nested, non-grouped, single-conditional selects
    like `SELECT * FROM table1 WHERE b > 3`
- Code Generation
  - chose which indexes to use when multiple available
  - chose loop order for joins.
  - simplify code using relational-algebra-like rules
