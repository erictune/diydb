Intermediate Representation and friends
=======================================

This database uses a number of different representation for SQL programs:
- SQL statements - strings of text
- a *Parse Tree*  (PT) - a tree of objects built by a generated parser, from
  generated types, closely matching the original SQL statements.  Uses a
  single enum for all PT types.  Retains character-level information.
- an *Abstract Syntax Tree* (AST) - a tree of hand-written objects.  Enums are
  narrowly defined simplifying match statements.  Discards character-level information.
  Some optimizations can be are performed at the AST level.  Types still map directly
  to SQL keywords.
- an *Intermediate Representation* (IR) - a tree (graph?) of hand-written objects
  that represent, roughly, the operations in the relational algebra: e.g. project.
  Does not closely map to SQL keywords.  Optimization performed at this level may
  combine or split operations.  Query planning will be performed on the IR.

The IR is currently interpreted. If bytecode or JIT code generation were implemented,
then it would generate code from the IR.

# Examples

# Tables used in examples

Table `t`:
```
create table t (a int, b int)
insert into t values (1,10)
insert into t values (2,20)
```

Index `t_a` as shown below:
```
create index t_a on t (a)
```

# IR Types
Some IR blocks have a child block.  These read in rows and produce rows.
The can be chained.
Some have several children, as in the case of a join or union.

|        type        | purpose                               |
| ------------------ | ------------------------------------- |
| `TempTable`        | holds a constant row or set of rows, not stored |
| `Scan`             | scans a stored table                  |
| `SeekRowid`        | finds a specific rowid in a stored table |
| `IndexedSeek`      | finds a specific row with the aid of an index |
| `Filter`           | returns only those rows that match an expression |
| `Project`          | returns only a subset of columns, and/or computes values on columns. |
| `Union`            | return rows from multiple sources |
| `Join`             | returns only a subset of columns, and/or computes values on columns. |
etc...

# Examples of SQL converted to IR

|  Preconditions | SQL Statement           |    IR    |   Notes |
| - | --------------- | ----------- | --------- |
| None | `select 1` | `ScanConstantRows` |  |
| Table `t` | `select * from t` | `Scan("t")` |  Selecting only * means we don't need a `Project` block. |
| Table `t` | `select * from t where rowid = 1` | `SeekRowid("t", rowid)` | |
| Table `t` | `select * from t where a = 1` | `Filter("a=1", Scan(t))` | |
| Table `t` | `select a from t` | `Project(["a"], Scan("t"))` | |
| Table `t` | `select b from t where rowid = 1` | `Project(["b"], RowidSeek("t", 1))` | |
| Table `t` | `select b from t where a = 1` | `Project(["b"], Filter("a=1", Scan(a)))` | |
| Table `t` and Index `t_a` | `select b from t where a = 1` | `Project(["b"], IndexedSeek("t", "t_a", "a=1"))` | |
| Table `t`  | `select a + a from t` | `Project(["a+a"], Scan(t)))` |  |
| None | `select 1 + 1`    | `TempTable([[2]])` | Constant expressions are simplified in the AST representation.  Select with no from are represented as a constant table. |

Note that expressions, which are written above for conciseness as e.g.`"a+1"`, are actually AST expression trees.

Here is a more complicated example.  This SQL:
```sql
select max(i,j) as k from (select a+1 as i, 2 * a as j from t)
```
turns into 3 IR blocks:
```
Obj #1:
Project(
    colnames: ["k"],  // list of names of the output columns.
    exprs: [BinOp::max(Column("i"),Column("j")]  // list of the expressions that generate the output columns.
    child: Box(#2)  // This points to obj #2.
)

Obj #2:
Project(
  colnames: ["i",
             "j"],
  exprs: [BinOp::add(Column("a"), Const::Int(1)),
          BinOp::mul(Column("a"), Const::Int(2))]
  child: Box(#3)
)

Obj #3:
Scan(
  colnames: ["a",
             "b",
             "c"],
  table_name: table_name,
)

# IR Optimization

IR optimizations might include:
- converting scans to seeks, index selection.
- deciding the order of joins
- splitting, combining, or moving projects.

I need to learn more about this.

# Interpreting IR

I considered building a data structure parallel to the IR that contains "execution blocks".
This turned out to be difficult to build in Rust - ownership gott to complex for me to follow.

Currently, only certain fixed sequences of IR blocks can be interpreted, and they are interpreted by building a
compile-time specified chain of iterators.  I'd like to get to the point where blocks can be chained together
at runtime and executed as a sort of dataflow graph. 

I've used streaming iterators instead of plain iterators.  The though process here was:
1) so the Project set can build a local row to return, and allow it to be used by reference,
   but not need to retain all computed rows.
2) to limit lifetime of borrows from scans to allow freeing/unlocking pages behind.)



One way to execute a dynamic graph of IR blocks is to advance each one and propagate the result to the next one,
as follows:

```rust
let blocks = blocks_sorted_from_bottom_to_top(blocks);
while let Some(row) = out {
    let item = None;
    for block in blocks {
        if out is None
            let out = true => block.get();
            block.advance()
        else
            let in = out;
            let out = block.get(in);
            block.advance();
    }
    emit(out);
}
```

But my Rust is not strong enough to make this work yet.