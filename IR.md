Intermediate Representation
====================

Design for IR for `diydb`.

See also [./ast.md] for the Abstract Syntax Tree.

See this discussion of having separate AST vs IR: https://www.querifylabs.com/blog/relational-operators-in-apache-calcite

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

# Enums vs Traits

-   Reference: https://www.possiblerust.com/guide/enum-or-trait-object
-   Traits are an open set of types.  Others can extend your code by implementing a trait.  Enums are a closed set of types.
    -   We don't need extensibility for our users.  For testing, we may want to define mock types.
-   Traits are a bit slower as they use vtables Enums are a bit faster as they use branchs.
    -   We prefer faster if possible.
-   You use an enum when you want to be forced to handle every alternative at every usage site.
    -   We don't want this.

# IR Traits

Trait names to be determined.  Whether to use common rust traits or new ones is TBD also.
TBD if traits are needed.

| tenative trait name |      trait description              | example IR types meeting it   |
| ------------------- | ----------------------------------- | ------------------------------ | 
| `LogicalOp`         | evaluation returns boolean          | `Eq`, `Gt`, `Ne`               |
| `RowIterable`       | evaluation returns sequence of rows | `ScanConstantRows`, `Scan`, `SeekRowid` |
| `NumericOp`         | evaluation returns numeric scalar   | `Negate`, `Add`                |
| `NullaryOp`         | evaluation takes a no arguments     | `ScanConstantRows`, `Scan`, `SeekRowid` |
| `UnaryOp`           | evaluation takes one scalar args    | `Negate`, `Sqrt`               |
| `BinaryOp`          | evaluation takes two scalar args    | `Add`, `Mul`                   |

# IR Types
These types hold some data (creation arguments), and can be evaluated

|       type         | creation arguments | eval input trail | eval output trait       | 
| ------------------ | ------------------ | ---------------- | ----------------------- |
| `ScanConstantRows` | immediate table    | `NullaryOp`      |  `RowIterable`          |
| `Scan`             | table name         | `NullaryOp`      |  `RowIterable`          |
| `SeekRowid`        | table name, rowid  | `NullaryOp`      |  `RowIterable`          |
| `IndexedSeekEq`    | ?                  | ?                |  `RowIterable`          |
| `Filter`           | reference to `LogicalOp`, reference to `RowIterable` | Row      |  `RowIterable`  (may not return every row)  -  |
| `Project`          | reference to `RowIterable` | Row      |  `RowIterable`          |
| `NewColFromExpr`   | ?                  | ?                |  `RowIterable`          |
| `Eq`, `Gt`, etc    | constants or exprs | ?                |  `LogicalOp`            |
| `Negate`           | reference to numeric const or numeric expr  |  ?  | ? |
| `Add`, `Mul`, etc  | ?  |  ?  | ? |

# Examples of SQL converted to IR


|  Preconditions | SQL Statement           |    IR    |   Notes |
| - | --------------- | ----------- | --------- |
| None | `select 1` | `ScanConstantRows` |  |
| Table `t` | `select * from t` | `Scan("t")` |  A star says we don't need a `Project`. |
| Table `t` | `select * from t where rowid = 1` | `SeekRowid("t", rowid)` |  | 
| Table `t` | `select * from t where a = 1` | `Filter(LogicalColExpr(Eq(Col("a"), Const(1, int)), Scan(a)` | `Filter` only returns rows from _arg2_ which match expression _arg1_. | 
| Table `t` | `select a from t` | `Project(["a"], Scan("t"))` | `Project` drops columns not mentioned in the column list (arg1) from table (arg2) |
| Table `t` | `select b from t where rowid = 1` | `Project(["b"], RowidSeek("t", 1))` | |
| Table `t` | `select b from t where a = 1` | `Project(["b"], Filter(Eq(Col("a"), Const(1, int)), Scan(a)))` | `Filter` only returns rows from _arg2_ which match expression _arg1_. | 
| Table `t` and Index `t_a` | `select b from t where a = 1` | `Project(["b"], IndexedSeekEq("t", "t_a", 1))` | `Filter` only returns rows from _arg2_ which match expression _arg1_. | 
| Table `t`  | `select a + a from t` | `Project(["_expr1"], NewColFromExpr("_expr1", Expr(Add(Col("a"), Col("a"))), Scan(t)))` |  NewColFromExpr adds a new column to table named _arg1_ to the table _arg3_ computed with expression _arg2_.   |
| None | `select 1 + 1`    | `ScanConstantRow(2)` | Constant expressions are evaluated before query exectution
| Table `t` | `select *, -a from t` | `AddColFromExpr("_expr1", Expr(UnaryMinus(Col("a"))), Scan(t))` | `AddColFromExpr` adds a new column to table named _arg1_ to the table _arg3_ computed with expression _arg2_. |
| Table `t` | `select a + a from t` | `Project(["_expr1"], AddColFromExpr("_expr1", Expr(UnaryMinus(Col("a"))), Scan(t)))` |  NewColFromExpr .   |
| Table `t` and Index `t_a` | `select * from t` | `Scan("t")` | No change. |
| Table `t` and Index `t_a` | `select * from t where rowid = 1` | `RowidSeek("t", rowid)` | No change | 
| Table `t` and Index `t_a` | `select * from t where a = 1` | `IndexSeekEq("t", "t_a", 1)` | `IndexSeekEq` returns the range of values in _arg1_ equal to _arg3_ using index table _arg2_. | 

# Questions and Observations
-   To stepwise evaluate a `Filter`, you have to check if it has a row ready right now, which is different from it being done.
-   An immediate table is needed to handle some sql expressions.  Also, an immediate table would help with testing SQL.  This requires
    a ScannableTable trait that can iterate over a constant or btree-based table. 
-   Is index selection part of AST to IR conversion, or  a subsequent steps (optimization, interpretation, codegen).
-   A complete implementation of `SeekRowid` would allow for expressions rather than a constant rowid, right?

