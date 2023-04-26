use std::borrow::BorrowMut;
use std::io::{self, BufRead, Write};

fn main() {
    let stdin = io::stdin();
    let mut c = Context { pager: None };
    println!("DIYDB - simple SQL database");
    println!("Enter .help for list of commands");
    print!("> ");
    io::stdout().flush().unwrap();
    for line in stdin.lock().lines() {
        match line {
            Ok(line) => do_command(c.borrow_mut(), line.as_str()),
            Err(e) => println!("Input error: {:}", e),
        }
        print!("> ");
        io::stdout().flush().unwrap();
    }
}

fn do_command(c: &mut Context, line: &str) {
    match line {
        l if l.to_uppercase().starts_with("SELECT") => do_select(c, l),
        l if l == ".schema" => do_schema(c),
        ".help" => do_help(c),
        l if l.starts_with(".open") => {
            let file_to_open = "./record.db";
            // TODO: parse one argument.
            do_open(c, file_to_open)
        }
        _ => println!("Unknown command."),
    }
}

struct Context {
    pager: Option<diydb::pager::Pager>,
}

fn do_help(_: &mut Context) {
    println!(
        "
.open               to open a persistent database.
.schema             to list the tables and their definitions.
SELECT ...          to do a query.
"
    );
}

fn do_open(c: &mut Context, path: &str) {
    // TODO: return errors from open
    match diydb::pager::Pager::open(path) {
        Ok(p) => { c.pager = Some(p); }
        Err(e) => {
            println!("Error opening database {path} : {}", e);
            return;
        }
    }
    match c.pager.as_mut() {
        Some(p) => {
            match p.initialize() {
                Ok(()) =>(),
                Err(e) => {
                    println!("Error initializing database {path} : {}", e);
                    return;
                }
            }
        }
        None => {
            println!("Unexpected condition.");

        },

    }
}

fn do_schema(c: &mut Context) {
    println!("Printing schema table...");
    match c.pager.as_mut() {
        Some(p) => diydb::print_schema(p.borrow_mut()),
        None => println!("Error, no database loaded"),
    }
}

fn do_select(c: &mut Context, l: &str) {
    println!("Doing query: {}", l);
    match c.pager.as_mut() {
        Some(pager) => diydb::run_query(&pager, l),
        None => println!("Error, no database loaded"),
    }
}
