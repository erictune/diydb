#! /bin/bash
# Script used to create golden db input files.
# Change these lines when copying to make a similar script.
prefix=schema_table
purpose="Test a schema table with several tables in it"


if test -f "${prefix}.db"; then
    echo "Remove existing db if regeneration is needed."
    exit -1
fi

(
echo "Purpose: Test a schema table with several tables in it"
echo "Tool version:"
sqlite3 --version
(
cat << EOF
.open $prefix.db
create table t1 (a int);
insert into t1 values (1);
create table t2 (a int, b int);
insert into t2 values (2,2);
create table t3 (a text, b int, c text, d int, e real);
insert into t3 values ("Three", 3, "3", 3, 3.0001);
EOF
) | sqlite3
) > $prefix.log
