//! What Flow reads in a harness's own records, shared by the hook that runs
//! inside the harness and the Nexus that settles what the hook reports: the
//! text Claude Code took when it took a paste, and the digest by which a
//! submitted prompt is matched to one queued request.

use sha2::{Digest, Sha256};

/// Claude Code's record of text that reached its composer as one paste.
///
/// Claude Code 2.1.294 writes it in one of two exact shapes (witnessed in
/// its transcripts): a prompt submitted from an idle composer as
/// `"\n\n<pasted_content id=\"ID\">\n" BODY "\n</pasted_content id=\"ID\">\n"`,
/// and a prompt that went through its input queue (an `enqueue` row, a
/// `queued_command` attachment, the user row a dequeue writes) without the
/// leading blank lines and the final newline. The identifier is not
/// interpreted; the opening and closing identifiers must be the same.
pub trait ReadsClaudePaste {
    /// The body of a whole-prompt paste; None for any other text.
    fn unwrapped_claude_paste(&self) -> Option<&str>;
    /// The text Claude Code took: a whole-prompt paste's body, else the
    /// prompt itself.
    fn submitted_claude_text(&self) -> &str;
}

impl ReadsClaudePaste for str {
    fn unwrapped_claude_paste(&self) -> Option<&str> {
        let (rest, closing_newline) = match self.strip_prefix("\n\n") {
            Some(rest) => (rest, "\n"),
            None => (self, ""),
        };
        let rest = rest.strip_prefix("<pasted_content id=\"")?;
        let (identifier, body) = rest.split_once("\">\n")?;
        if identifier.is_empty() || identifier.contains(['\n', '"']) {
            return None;
        }
        body.strip_suffix(
            format!("\n</pasted_content id=\"{identifier}\">{closing_newline}").as_str(),
        )
    }

    fn submitted_claude_text(&self) -> &str {
        self.unwrapped_claude_paste().unwrap_or(self)
    }
}

/// The SHA-256 of a prompt's exact bytes, lowercase hex: equal digests are
/// an exact match of the whole prompt, so no prompt text has to travel.
pub trait DigestsPrompt {
    fn prompt_sha256(&self) -> String;
}

impl DigestsPrompt for str {
    fn prompt_sha256(&self) -> String {
        format!("{:x}", Sha256::digest(self.as_bytes()))
    }
}

#[cfg(test)]
mod tests {
    use super::{DigestsPrompt, ReadsClaudePaste};

    const BODY: &str = "#msg [\"psyche core Secondary\" \"go\"]";

    #[test]
    fn both_native_paste_shapes_unwrap_to_the_exact_body() {
        let idle =
            format!("\n\n<pasted_content id=\"04ee\">\n{BODY}\n</pasted_content id=\"04ee\">\n");
        let queued = format!("<pasted_content id=\"00df\">\n{BODY}\n</pasted_content id=\"00df\">");
        assert_eq!(idle.unwrapped_claude_paste(), Some(BODY));
        assert_eq!(queued.unwrapped_claude_paste(), Some(BODY));
        assert_eq!(BODY.submitted_claude_text(), BODY);
    }

    #[test]
    fn a_partial_or_mismatched_wrapper_is_not_unwrapped() {
        for text in [
            format!("<pasted_content id=\"a\">\n{BODY}\n</pasted_content id=\"b\">"),
            format!("\n\n<pasted_content id=\"a\">\n{BODY}\n</pasted_content id=\"a\">"),
            format!("<pasted_content id=\"a\">\n{BODY}\n</pasted_content id=\"a\">\n"),
            format!("typed first <pasted_content id=\"a\">\n{BODY}\n</pasted_content id=\"a\">"),
            format!("<pasted_content id=\"\">\n{BODY}\n</pasted_content id=\"\">"),
        ] {
            assert_eq!(text.unwrapped_claude_paste(), None, "{text:?}");
            assert_eq!(text.submitted_claude_text(), text);
        }
    }

    #[test]
    fn the_digest_is_sha256_of_the_exact_text() {
        assert_eq!(
            "abc".prompt_sha256(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_ne!("abc".prompt_sha256(), "abc\n".prompt_sha256());
    }
}
