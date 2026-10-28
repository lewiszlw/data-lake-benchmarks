// SPDX-License-Identifier: MIT

//! Phase selection and the machine-readable result format.

use std::env;

use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Cli {
    pub phase: String,
    pub run_id: String,
}

pub fn parse_cli(binary: &str, usage: &str) -> Cli {
    let args: Vec<String> = env::args().collect();
    let phase = args.get(1).cloned().unwrap_or_else(|| "query".to_string());
    if phase == "-h" || phase == "--help" {
        println!("usage: {binary} {usage}");
        std::process::exit(0);
    }
    let run_id = args
        .get(2)
        .cloned()
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    Cli { phase, run_id }
}

/// Emit one machine-readable result line. The scripts parse these lines and
/// `docs/RESULTS.md` documents the stable key set.
pub fn emit(engine: &str, phase: &str, fields: &[(&str, String)]) {
    let mut line = format!("RESULT engine={engine} phase={phase}");
    for (key, value) in fields {
        line.push(' ');
        line.push_str(key);
        line.push('=');
        line.push_str(value);
    }
    println!("{line}");
}
