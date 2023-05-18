#! /bin/bash
# Script used to create golden db input files.
# Change these lines when copying to make a similar script.
prefix=for_exprs
purpose="A table for testing projects and expression evaluation"
sqlite3="/Users/etune/homebrew/Cellar/sqlite/3.34.1/bin/sqlite3"

if test -f "${prefix}.db"; then
    echo "Remove existing db if regeneration is needed."
    exit -1
fi

(
echo "Purpose: Test a schema table with several tables in it"
echo "Tool version:"
$sqlite3 --version
(
cat << EOF
.open $prefix.db
CREATE TABLE t (a int, b int, c real, d real, e text, f text);
INSERT INTO t VALUES(1, 1, 1.1, 1.1, "A", "A");
INSERT INTO t VALUES(1, 2, 1.1, 2.2, "A", "B");
INSERT INTO t VALUES(2, 1, 2.2, 1.1, "B", "A");
INSERT INTO t VALUES(0, 3, 0.0, 3.3, "A", "A");
EOF
) | $sqlite3
) > $prefix.log
