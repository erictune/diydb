#! /bin/bash
# Script used to create golden db input files.
# Change these lines when copying to make a similar script.
prefix=threelevel
purpose="Test a table with enough pages in it to have 3 btree levels"


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
create table letters (l text);
insert into letters values ("A"), ("B"), ("C"), ("D"), ("E"), ("F"), ("G"), ("H"), ("I"), ("J");
create table t (v text,w text,x text,y text,z text);
insert into t (v,w,x,y,z) select * from letters a cross join letters b cross join letters c cross join letters d cross join letters e;
EOF
) | sqlite3
) > $prefix.log
