/// The ordinary client accepts exactly one Datom request, e.g. Start.{ codex-medium «goal» ... }.
fn main() {
    let mut arguments = std::env::args().skip(1);
    let Some(_datom) = arguments.next() else {
        std::process::exit(2)
    };
    if arguments.next().is_some() {
        std::process::exit(2)
    }
}
