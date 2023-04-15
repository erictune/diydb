// TODO: main should just be a REPL like the sqlite3 cli.
fn main() {
    let mut pager = diydb::pager::Pager::open("./record.db");
    pager.initialize();
    println!("-----");
    println!("Printing schema table...");
    diydb::print_schema(&pager);
    println!("-----");
    let q = "SELECT * FROM record_test";
    println!("Doing query: {}", q);
    diydb::run_query(&pager, q);
}
