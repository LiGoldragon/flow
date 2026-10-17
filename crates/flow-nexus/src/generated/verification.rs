//! The hand-written side of the sealed [`VerifiedContext`]: its fields are
//! hidden from every module but `generated`, so the one constructor,
//! [`VerifiesContext::verified`], and the readers live here, beside the
//! generated module. A composed context Flow has verified is the guard that
//! keeps any text but the composer's verified answer out of a prompt.

use super::curriculum::{ContextProvenance, StartupPlacement};
use super::starter::{
    ContextRequesting, CutsRenderedText, ReadsVerifiedContext, StartupComposing, VerifiedContext,
    VerifiesContext,
};
use sha2::{Digest, Sha256};
use signal_flow::{Address, ContextRefusal};

impl VerifiesContext for VerifiedContext {
    fn verified<N: ContextRequesting, O: StartupComposing>(
        input_0: N,
        input_1: O,
    ) -> Result<Self, ContextRefusal> {
        let request = input_0.context_request();
        let composition = input_1.composition();
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        if digest(&composition.system_text) != composition.system_payload_sha256
            || digest(&composition.user_text) != composition.user_payload_sha256
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let rows = &composition.context_provenance_vector;
        let placed = |placement: StartupPlacement| -> Vec<ContextProvenance> {
            rows.iter()
                .filter(|row| row.startup_placement == placement)
                .cloned()
                .collect()
        };
        let system_rows = placed(StartupPlacement::System);
        let user_rows = placed(StartupPlacement::User);
        let present = |identities: &[String], placed: &[ContextProvenance]| {
            identities
                .iter()
                .all(|identity| placed.iter().any(|row| &row.module_identity == identity))
        };
        if !present(&request.system_modules, &system_rows)
            || !present(&request.user_modules, &user_rows)
        {
            return Err(ContextRefusal::ModuleMissing);
        }
        let ordered = |placed: &[ContextProvenance]| {
            placed
                .iter()
                .enumerate()
                .all(|(ordinal, row)| row.integer == ordinal as i64)
        };
        if !ordered(&system_rows) || !ordered(&user_rows) {
            return Err(ContextRefusal::PayloadMismatch);
        }
        if composition
            .system_text
            .rendered_segments(&system_rows)
            .is_none()
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let user_segments = composition
            .user_text
            .rendered_segments(&user_rows)
            .ok_or(ContextRefusal::PayloadMismatch)?;
        Ok(Self {
            address: request.address.clone(),
            system_text: composition.system_text,
            user_segments,
        })
    }
}

/// What a verified context holds: the Address it was composed for, the
/// system text, and the USER text cut where each module begins, the first
/// segment holding the opening lines with the first module. The segments'
/// concatenation is the USER text exactly; there are none when no module is
/// USER placed.
impl ReadsVerifiedContext for VerifiedContext {
    fn address(&self) -> Address {
        self.address.clone()
    }

    fn system_text(&self) -> String {
        self.system_text.clone()
    }

    fn user_segments(&self) -> Vec<String> {
        self.user_segments.clone()
    }
}
