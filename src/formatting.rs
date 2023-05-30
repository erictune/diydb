//! formatting prints out tables nicely.

use anyhow::Result;

/// Printing out tables nicely.
/// In the future, also csv output, etc.

pub fn print_table_tt(tt: &crate::TempTable, detailed: bool) -> Result<()> {
    println!(
        "   | {} |",
        tt.column_names
            .iter()
            .map(|x| format!("{:15}", x))
            .collect::<Vec<String>>()
            .join(" | ")
    );
    if detailed {
        println!(
            "   | {} |",
            tt.column_types
                .iter()
                .map(|x| format!("{:15}", x))
                .collect::<Vec<String>>()
                .join(" | ")
        );
    }
    {
        for tr in tt.rows.iter() {
            println!(
                "   | {} |",
                tr.items
                    .iter()
                    .map(|x| format!("{:15}", x))
                    .collect::<Vec<String>>()
                    .join(" | ")
            );
        }
    }
    Ok(())
}
