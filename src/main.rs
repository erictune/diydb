use std::io::{self, BufRead, Write};

fn main() {
    let mut c: Context = Context {
        pagerset: diydb::pager::PagerSet::new(),
    };
    let stdin = io::stdin();
    println!("DIYDB - simple SQL database");
    println!("Enter .help for list of commands");
    print!("> ");
    io::stdout().flush().unwrap();
    for line in stdin.lock().lines() {
        match line {
            Ok(line) => do_command(&mut c, line.as_str()),
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
    pagerset: diydb::pager::PagerSet,
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
    match c.pagerset.opendb(path) {
        Ok(()) => {}
        Err(e) => {
            println!("Error opening database {path} : {}", e);
        }
    }
}

fn do_schema(c: &mut Context) {
    println!("Printing schema table for default database...");
    match c.pagerset.default_pager() {
        Ok(p) => {
            if let Err(e) = diydb::print_schema(p) {
                println!("Error printing schemas: {}", e);
            }
        }
        Err(e) => println!("Error accessing default database (maybe none loaded?) : {e}"),
    }
}

fn do_select(c: &mut Context, l: &str) {
    println!("Doing query: {}", l);
    if let Err(e) = diydb::run_query(&c.pagerset, l) {
        println!("Error running query: {}", e);
    }
}
