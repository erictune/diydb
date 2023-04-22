# DIY Database
`diydb` is a toy Database modeled after Sqlite, written in Rust as a learning exercise.

# Goals
For the author to:
- learn Rust
- re-learn b-trees
- explore basic database optimizations

# Non-goals
- Careful compatibility with SQLite
- Making a database that is useful to others, beyond learning.
- Distributed system.

# Current State

- Can open some very simple sqlite database files and dump the contents.
  - We only handle pages of type btree, not e.g. free lists.  This is still usable, since when Sqlite files are created from sessions that use only CREATE TABLE and INSERT statements, the resulting files don't have other page types.
- No writing yet.  Inputs are created using `sqlite3` CLI.
- No demand paging.

# Code Structure

When starting out, I tried to emulate Sqlite's architecture (https://www.sqlite.org/arch.html), which looks like this:

- Interface
- SQL Command Processor
  - Tokenizer
  - Parser
  - Code Generator
- Virtual Machine
- Btree
- Pager
- OS Interface

Thee current state looks like this:

- Interface - *REPL with just a few commands.*
- SQL Command Processor
  - Tokenizer and Parser - *Lexing and parsing are combined in Parsing Expression Grammars, which pest.rs uses. Produces a Parse Tree (PT).*
  - Abstract Symbol Table (AST) - *A PT has one enum for all terminals.  The AST has separate enums for subsets of terminals.*
  - AST Optimization - *Planned.  For example, constant folding and propagation.*
  - Intermediate Representation (IR) - *Planned.  The IR graph has types for different elements for different runtime operations like Table Scan, Seek with Index, Seek with Row ID.*
  - IR Optimization - *Planned.*
- Execution
  - Interpreter
  - Virtual Machine - *Not planning to implementa bytecode VM*
  - JIT - *Aspire to implement JIT of queries.*
- B-Tree - *Covers key-value storage, without interpreting values as rows.*
- Pager - *Minimal*
  - Lock-based Concurrency Control - *Intend to implement*
  - Multiversion (MVCC) - *Not planned*
- OS Interface - *No, not interested in multiple OS support*

Files are organized as follows:
* Interface layer
    * `main.rs` - Basic REPL
    * `formatting.rs` - prints out tables nicely.
* SQL Command Processor
    * `sql.pest` - Defines grammar for parser.
    * `parser.rs` - Module holds generated parser [https://pest.rs/] and tests for grammar.
    * `pt_to_ast.rs` - Functions to convert parse tree to abstract syntax tree.
* Execution
  * `serial_types.rs` - handles SQLite *serial types* (which can differ from row to row within a column, and are different from SQL types).
  * `record.rs` - iterates over and parses row records that are stored in btree cells.
* B-Tree
  * `btree/*.rs` - provides iterators to walk over btree elements.  Uses a Pager to get at pages.
* Pager
  * `pager.rs` - provides interface to get a page of the DB for reading.  In the future, it may or may not be present in memory  when requested.  It holds the handle to the open database file.

# Future Work
See also [TODO.md](./TODO.md).

In no particular order.
- Data
  - Support for scanning multi-page btrees.
  - Support searching within multi-page btrees, rather than just scanning.
  - Support indexes.
  - Support blobs
  - Support overflow
- Concurrency
  - Support basic concurrency with at least Table-level locking. No plan for MVCC.
  - Pager layer to support multiple writers of different tables, which will require some unsafe rust to
    hide the locking done underneath.
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
  - Consistency
    - Write rollback journal, write the transaction, and then delete the rollback journal.
- Parsing
  - Selection of specific columns from tables.
  - `WHERE` clauses in SQL statements.
  - `JOIN`
  - `GROUP BY`
  - nested select (maybe?)
- Code Generation
  - chose which indexes to use when multiple available
  - chose loop order for joins.
  - simplify code using relational-algebra-like rules

# Similar Projects

- https://github.com/erikgrinaker/toydb

# Notable Things I Learned

## When to use Traits vs Enums

-   Reference: https://www.possiblerust.com/guide/enum-or-trait-object
-   You use an enum when you want to be forced to handle every alternative at every usage site.
-   You use Traits when you want to have an open-ended set of types you can use.
    -   Others can extend your code by implementing a trait.  Enums are a closed set of types.
    -   If you don't need extensibility for users, you can skip using traits.  But, for testing, one may want to define mock types
        for internal use that use traits.
-   Traits are a bit slower as they use vtables Enums are a bit faster as they use branchs.
    -   We prefer faster if possible.
    -   In some cases the compiler can eliminate the need for this, I think.

## Lifetime Specifiers
It has been useful to reminding myself that the lifetime specifier is not the "places where this reference is used" (scope).
Rather it is the lifetime of the variable (referrent).  In one failed attempt at using lifetimes, I added more bounds for a
type with several references, but actually both references were to the same variable (the pager and its data).  Sometimes
adding the compilers suggestions it the right things, but other times it is not.

