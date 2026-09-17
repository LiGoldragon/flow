/// The meta client accepts exactly one Datom Configure request.
fn main() {
    if std::env::args().skip(1).count() != 1 {
        std::process::exit(2)
    }
}
