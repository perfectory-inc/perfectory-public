// Fixture for tools/semgrep/rules/no-hand-cleared-gate-file.yml (`opengrep scan --test`). Not built.

fn the_2026_11_07_court_auction_stop(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        // ruleid: no-run-waits-for-a-person-to-remove-a-file
        bail!("courtauction.go.kr refused an earlier run (ipcheck=false); no run starts until a person checks it and removes {}", path.display());
    }
    Ok(())
}

fn the_rt_molit_stop(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        bail!(
            // ruleid: no-run-waits-for-a-person-to-remove-a-file
            "rt.molit.go.kr refused an earlier run ({}); no request is sent until a person reads it \
             and removes the file",
            path.display()
        );
    }
    Ok(())
}

// ruleid: no-run-waits-for-a-person-to-remove-a-file
// A collector stops at once and does not ask again until an operator clears it.
fn doc_of_a_stop() {}

// ok: no-run-waits-for-a-person-to-remove-a-file
// 2026-11-06: every later run refused to start until a person removed refused.json.
fn history_in_the_past_tense() {}

// ok: no-run-waits-for-a-person-to-remove-a-file
// No file stops a run and no person clears anything; the run waits and asks again.
fn the_accepted_form() {}
