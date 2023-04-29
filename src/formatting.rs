//! formatting prints out tables nicely.

use anyhow::Result;

/// Printing out tables nicely.
/// In the future, also csv output, etc.

pub fn print_table_qot(qot: &crate::QueryOutputTable, detailed: bool) -> Result<()> {
    println!(
        "   | {} |",
        qot.column_names
            .iter()
            .map(|x| format!("{:15}", x))
            .collect::<Vec<String>>()
            .join(" | ")
    );
    if detailed {
        println!(
            "   | {} |",
            qot.column_types
                .iter()
                .map(|x| format!("{:15}", x))
                .collect::<Vec<String>>()
                .join(" | ")
        );
    }
    {
        for tr in qot.rows.iter() {
            print!("{:2} |", tr.row_id);
            for v in tr.items.iter() {
                print!(" {:15} |", v);
            }
            println!();
        }
    }
    Ok(())
}
