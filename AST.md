Abstract Syntax Tree
====================

Design for AST for `diydb`.

See this example of building an AST from a `pest.rs` Parse Tree: https://github.com/ehsanmok/create-your-own-lang-with-rust/blob/master/calculator/src/parser.rs

It shows that we need to:
- Define enums for all the possible AST nodes.
- Write code to convert the messy dynamically typed parse tree into an enum-based AST.

TODO: define the nodes we will support in our parse tree initially.  Suggest at least:

- common integer operations
- `select`, `from`, and `where` clauses
- table and column names
- integer constants
- look at the examples in [./IR.md]

The AST is then converted into an IR.  See also [./IR.md] for the Intermediate Representation.

