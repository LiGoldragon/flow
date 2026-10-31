// usage: truncate_table <store> <table>...   (removes every row; keeps the table and its registration)
fn main() {
    let mut args = std::env::args().skip(1);
    let path = args.next().unwrap();
    let db = redb::Database::open(&path).unwrap();
    let write = db.begin_write().unwrap();
    for name in args {
        let def = redb::TableDefinition::<String, &[u8]>::new(Box::leak(name.clone().into_boxed_str()));
        let mut table = write.open_table(def).unwrap();
        let mut keys = Vec::new();
        for entry in redb::ReadableTable::iter(&table).unwrap() {
            keys.push(entry.unwrap().0.value());
        }
        for key in &keys { table.remove(key).unwrap(); }
        eprintln!("{name}: removed {} rows", keys.len());
    }
    write.commit().unwrap();
}
