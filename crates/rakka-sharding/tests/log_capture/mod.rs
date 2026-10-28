//! Captures `tracing` output so a test can assert what `rakka-sharding` logs.
//!
//! The subscriber is installed once per test binary at `WARN` and above; every
//! test in the binary then writes into the same buffer, so assertions should
//! select the lines they care about by their message text.

#![allow(dead_code)]

use std::io::{self, Write};
use std::sync::{Arc, Mutex, OnceLock};

/// Shared buffer holding every captured log line.
#[derive(Clone, Default)]
pub struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl CapturedLogs {
    /// Every captured line so far.
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.0.lock().expect("captured logs mutex poisoned")).into_owned()
    }

    /// The captured lines whose text contains `needle`.
    pub fn lines_containing(&self, needle: &str) -> Vec<String> {
        self.text()
            .lines()
            .filter(|line| line.contains(needle))
            .map(str::to_owned)
            .collect()
    }
}

struct CaptureWriter(CapturedLogs);

impl Write for CaptureWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
             .0
            .lock()
            .expect("captured logs mutex poisoned")
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Installs the capturing subscriber (once) and returns the shared buffer.
pub fn install() -> CapturedLogs {
    static CAPTURED: OnceLock<CapturedLogs> = OnceLock::new();
    CAPTURED
        .get_or_init(|| {
            let captured = CapturedLogs::default();
            let for_writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_ansi(false)
                .with_max_level(tracing::Level::WARN)
                .with_writer(move || CaptureWriter(for_writer.clone()))
                .finish();
            let _already_set = tracing::subscriber::set_global_default(subscriber);
            captured
        })
        .clone()
}
