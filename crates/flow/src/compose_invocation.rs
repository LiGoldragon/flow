use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow::Invocation;
use flow_ethos::OrdinarySocketPath;
use protos::{Compactable, Protosizable, ReaderBudget};
use signal_flow::Query;
use std::env;

fn main() {
    let mut arguments = env::args().skip(1);
    let (Some(socket), Some(query), None) = (arguments.next(), arguments.next(), arguments.next())
    else {
        eprintln!("usage: flow-compose-invocation '<OrdinarySocketPath>' '<Query>'");
        std::process::exit(2)
    };
    let budget = || Budget {
        remaining: 65_536,
        reader: ReaderBudget { remaining: 65_536 },
        depth: 0,
        maximum_depth: 1_024,
    };
    let invocation = (|| {
        let ordinary_socket_path = Potential::<OrdinarySocketPath>::from(socket)
            .actualize(&mut budget())
            .map_err(|error| format!("invalid ordinary socket path: {error:?}"))?;
        let query = Potential::<Query>::from(query)
            .actualize(&mut budget())
            .map_err(|error| format!("invalid ordinary Query: {error:?}"))?;
        Ok::<_, String>(Invocation {
            ordinary_socket_path,
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
