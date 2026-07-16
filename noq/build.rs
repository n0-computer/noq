use cfg_aliases::cfg_aliases;

#[allow(
    semicolon_in_expressions_from_macros,
    reason = "cfg_aliases needs an update: https://github.com/katharostech/cfg_aliases/pull/15"
)]
fn main() {
    // Setup cfg aliases
    cfg_aliases! {
        // Convenience aliases
        wasm_browser: { all(target_family = "wasm", target_os = "unknown") },
    }
}
