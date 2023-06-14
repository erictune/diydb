use std::io::{self, BufRead, Write};

fn main() {
    let mut c: Context = Context {
        pagerset: diydb::pager::PagerSet::new(),
    };
    let stdin = io::stdin();
    println!("DIYDB - simple SQL database");
    println!("Enter .help for list of commands");
    print!("diydb> ");
    io::stdout().flush().unwrap();
    let mut stdin_iter = stdin.lock().lines().into_iter();
    'outer: while let Some(result) = stdin_iter.next() {
        let mut line = match result {
            Ok(line) => line,
            Err(e) => { println!("Input error: {:}", e); continue; },
        };
        // Gather additional lines if multi-line command.
        // Commands that start with "." are always single line.
        // Commands that don't start with "." are terminated with semicolon
        // either on the first line or other lines.
        if !line.as_str().starts_with(".") && !line.as_str().ends_with(";") {
            'inner: loop {
                print!("  ...> ");
                io::stdout().flush().unwrap();
                let extra_line = match stdin_iter.next() {
                    None => {
                        println!("End of input during multi-line command");
                        break 'outer;
                    }
                    Some(extra_result) => {
                        match extra_result {
                            Ok(extra_line) => extra_line,
                            Err(e) => {
                                println!("Input error during multi-line command: {:}", e);
                                break 'inner;
                            },
                        }
                    }
                };
                // Append the extra line to the preceding lines, space-separated.
                line.push_str(" ");
                line.push_str(&extra_line);
                if line.ends_with(";") {
                    break 'inner;
                } else {
                    continue
                }
            }
        } 
        // A line or lines of input are collected; run the command.
        do_command(&mut c, line.as_str());
        // Prompt for the next command.
        print!("diydb> ");
        io::stdout().flush().unwrap();
    }
}

fn do_command(c: &mut Context, line: &str) {
    if line.len() == 0 {
        println!("Empty command.");
        return;
    }
    // Dot commands.
    if let Some('.') = line.chars().nth(0)  {
        match line {
            ".schema" => do_schema(c),
            ".help" => do_help(c),
            l if l.starts_with(".open") => {
                if let Some((_, file_to_open)) = line.split_once(" ") {
                do_open(c, file_to_open)
                } else {
                    println!("Unspecified filename.");
                }
            }
            _ => println!("Unknown command (2): `{}`", line),
        }
            return;
    }
    // SQL commands
    let first_word = line.split_ascii_whitespace().next();
    if first_word.is_none() {
        println!("Unknown SQL command: `{}`", line);
        return;
    }
    if !line.ends_with(";") {
        // Semicolon are considered statement separators in SQL, so they are apparently not required for
        // API calls, or for places where SQL is stored, like the schema table.  But, they are used to end
        // possibly multi-line statements in interactive mode, which this is.
        println!("SQL statements must end with a semicolon.");
        return;
    }
    // Remove semicolon for parsing.
    let line = &line[0..line.len()-1];
    let first_word = first_word.unwrap().to_uppercase();
    match first_word.as_str() {
        "SELECT" => {
            do_select(c, line)
        }
        "INSERT" => {
            do_insert(c, line)
        }
        _ => println!("Unknown SQL command: `{}`", line),
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
INSERT ...          to insert values into a table.
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
    if let Err(e) = diydb::run_query(&c.pagerset, l) {
        println!("Error running query: {}", e);
    }
}

fn do_insert(c: &mut Context, l: &str) {
    if let Err(e) = diydb::run_insert(&mut c.pagerset, l) {
        println!("Error running statement: {}", e);
    }
}