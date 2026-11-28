use flow_recover::{PlacesSessions, PrintsDatom, ReadsDatom, Recovery};
use std::process::ExitCode;

/// `flow-recover '<Recovery>'`: one datom in, one Placement datom per line
/// out. It reads no environment and sends nothing; its lines are for
/// flow-meta.
fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    let (Some(source), None) = (arguments.next(), arguments.next()) else {
        eprintln!("usage: flow-recover '<one Recovery datom>'");
        return ExitCode::from(2);
    };
    let recovery = match Recovery::from_datom_text(&source) {
        Ok(recovery) => recovery,
        Err(error) => {
            eprintln!("invalid Recovery datom: {error}");
            return ExitCode::from(2);
        }
    };
    match recovery.placements() {
        Ok(placements) => {
            for placement in placements {
                println!("{}", placement.datom_text());
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
