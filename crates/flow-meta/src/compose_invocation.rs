use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_ethos::MetaSocketPath;
use flow_meta::Invocation;
use meta_signal_flow::Query;
use protos::{Compactable, Protosizable, ReaderBudget};
use std::env;

fn main() {
    let mut arguments = env::args().skip(1);
    let (Some(socket), Some(query), None) = (arguments.next(), arguments.next(), arguments.next())
    else {
        eprintln!("usage: flow-meta-compose-invocation '<MetaSocketPath>' '<Query>'");
        std::process::exit(2)
    };
    let budget = || Budget {
        remaining: 65_536,
        reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
        depth: 0,
        maximum_depth: 1_024,
    };
    let invocation = (|| {
        let meta_socket_path = Potential::<MetaSocketPath>::from(socket)
            .actualize(&mut budget())
            .map_err(|error| format!("invalid meta socket path: {error:?}"))?;
        let query = Potential::<Query>::from(query)
            .actualize(&mut budget())
            .map_err(|error| format!("invalid meta Query: {error:?}"))?;
        Ok::<_, String>(Invocation {
            meta_socket_path,
            query,
        })
    })();
    match invocation {
        Ok(invocation) => println!("{}", invocation.datomize(vec![]).protosize().compact()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}
