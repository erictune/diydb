//! formatting prints out tables nicely.

use crate::record::{HeaderIterator, ValueIterator};
use crate::serial_type::{typecode_to_string, value_to_string};

/// Printing out tables nicely.
/// In the future, also csv output, etc.

// TODO: this should take a trait or traits, rather than a specific type of iterator, to allow testing with mock tables.
pub fn print_table(
    record_iterator: &mut crate::btree::table::Iterator,
    table_name: &str,
    col_names: Vec<&str>,
    col_types: Vec<&str>,
    detailed: bool,
) {
    println!("Full Dump of Table {}", table_name);
    println!(
        "   | {} |",
        col_names
            .iter()
            .map(|x| format!("{:15}", x))
            .collect::<Vec<String>>()
            .join(" | ")
    );
    if detailed {
        println!(
            "   | {} |",
            col_types
                .iter()
                .map(|x| format!("{:15}", x))
                .collect::<Vec<String>>()
                .join(" | ")
        );
    }
    {
        for (rowid, payload) in record_iterator {
            let rhi = HeaderIterator::new(payload);
            if detailed {
                print!("{:2} |", rowid);
                for t in rhi {
                    print!(" {:15} |", typecode_to_string(t));
                }
                println!("");
            }
            print!("{:2} |", rowid);
            let hi = ValueIterator::new(&payload[..]);
            for (t, v) in hi {
                print!(" {:15} |", value_to_string(&t, v));
            }
            println!("");
        }
    }
}
