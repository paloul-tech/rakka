//! What the documentation-currency tests share: locating the repository,
//! reading a file by repository-relative path, and the two Markdown readers
//! every table-holding test needs.
//!
//! These tests hold `docs/*.md` to the code, so each one reads files outside
//! its crate and walks Markdown tables. The helpers lived as copies in each
//! file until a review counted them; one module means one place to get the
//! fence handling right.

// Each test binary compiles this module independently and uses a subset of it.
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

/// The repository root, two levels above this crate's manifest.
pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rakka-agent manifest should live under crates/rakka-agent")
        .to_path_buf()
}

/// A file read by repository-relative path.
pub fn read(relative: &str) -> String {
    let path = repo_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("failed to read {relative}: {error}"))
}

/// The text after a heading, up to the next heading of any level.
///
/// A `#` that opens a line inside a fenced code block is a shell comment, not
/// a heading, so fences are tracked and never end a section.
pub fn section<'a>(document: &'a str, heading: &str) -> &'a str {
    let start = document
        .find(heading)
        .unwrap_or_else(|| panic!("the document has no heading {heading:?}"));
    let rest = &document[start + heading.len()..];
    let mut in_fence = false;
    let mut end = 0;
    for line in rest.split_inclusive('\n') {
        if line.trim_start().starts_with("```") {
            in_fence = !in_fence;
        } else if !in_fence && line.starts_with('#') {
            return &rest[..end];
        }
        end += line.len();
    }
    rest
}

/// The backticked tokens of one cell or passage, in order.
pub fn backticked(text: &str) -> Vec<String> {
    text.split('`')
        .skip(1)
        .step_by(2)
        .map(str::to_string)
        .collect()
}

/// Every `.rs` file directly under a repository-relative directory, as
/// `(relative path, source)`, sorted by path. `relative_dir` ends in `/`.
pub fn rust_files(relative_dir: &str) -> Vec<(String, String)> {
    let dir = repo_root().join(relative_dir);
    let mut files = Vec::new();
    for entry in fs::read_dir(&dir).unwrap_or_else(|error| panic!("read {relative_dir}: {error}")) {
        let path = entry.expect("a directory entry is readable").path();
        if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a source file has a name");
        let relative = format!("{relative_dir}{name}");
        files.push((relative.clone(), read(&relative)));
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}
