#! /bin/bash
# Script used to create golden db input files.
# Change these lines when copying to make a similar script.
prefix=multipage
purpose="Test a table with several pages in it"


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
create table thousandrows (x text, y text, z text);
insert into thousandrows (x,y,z) select * from letters a cross join letters b cross join letters c;
EOF
) | sqlite3
) > $prefix.log
