#![forbid(unsafe_code)]

//! Child-process fixture executable for `rakka-process` integration tests.

use std::io::{BufRead, Read, Write};
use std::time::Duration;

fn main() {
    let Some(command) = std::env::args().nth(1) else {
        eprintln!("missing fixture command");
        std::process::exit(2);
    };

    match command.as_str() {
        "fixture_waits_for_stdin_eof" => waits_for_stdin_eof(),
        "fixture_ignores_stdin" => ignores_stdin(),
        "fixture_exits_with_status_17" => exits_with_status_17(),
        "fixture_exits_after_delay" => exits_after_delay(),
        "fixture_asserts_environment_policy" => asserts_environment_policy(),
        "fixture_raw_echo" => raw_echo(),
        "fixture_line_json_echo" => line_json_echo(),
        "fixture_line_json_delayed" => line_json_delayed(),
        "fixture_line_json_malformed" => line_json_malformed(),
        "fixture_line_json_crash" => line_json_crash(),
        unknown => {
            eprintln!("unknown fixture command: {unknown}");
            std::process::exit(2);
        }
    }
}

fn waits_for_stdin_eof() {
    let mut stdin = std::io::stdin();
    let mut buffer = Vec::new();
    stdin
        .read_to_end(&mut buffer)
        .expect("stdin should be readable");
}

fn ignores_stdin() {
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn exits_with_status_17() {
    std::process::exit(17);
}

fn exits_after_delay() {
    std::thread::sleep(Duration::from_millis(30));
    std::process::exit(17);
}

fn asserts_environment_policy() {
    assert_eq!(
        std::env::var("RAKKA_DECLARED_TEST").as_deref(),
        Ok("present")
    );
    assert!(
        std::env::var_os("PATH").is_none(),
        "undeclared parent environment should not be inherited"
    );
}

fn raw_echo() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().split(b'\n') {
        let mut line = line.expect("stdin line should be readable");
        line.push(b'\n');
        stdout.write_all(&line).expect("stdout should be writable");
        stdout.flush().expect("stdout should flush");
    }
}

fn line_json_echo() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line.expect("stdin line should be readable");
        let frame = json_frame(&line);
        let id = json_id(&frame);
        eprintln!("line-json:{id}");
        write_json_response(&mut stdout, id, frame.get("payload"));
    }
}

fn line_json_delayed() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = line.expect("stdin line should be readable");
        let frame = json_frame(&line);
        std::thread::sleep(Duration::from_millis(120));
        write_json_response(&mut stdout, json_id(&frame), frame.get("payload"));
    }
}

fn line_json_malformed() {
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    let mut lines = stdin.lock().lines();
    let _line = lines
        .next()
        .expect("stdin should receive a request")
        .expect("stdin line should be readable");
    writeln!(stdout, "not-json").expect("stdout should be writable");
    stdout.flush().expect("stdout should flush");
}

fn line_json_crash() {
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let _line = lines
        .next()
        .expect("stdin should receive a request")
        .expect("stdin line should be readable");
    std::process::exit(17);
}

fn json_frame(line: &str) -> serde_json::Value {
    serde_json::from_str(line).expect("line-json frame should parse")
}

fn json_id(frame: &serde_json::Value) -> &str {
    frame
        .get("id")
        .and_then(serde_json::Value::as_str)
        .expect("line-json frame should have id")
}

fn write_json_response(
    stdout: &mut std::io::Stdout,
    id: &str,
    payload: Option<&serde_json::Value>,
) {
    let response = serde_json::json!({
        "id": id,
        "payload": payload.cloned().unwrap_or(serde_json::Value::Null),
    });
    writeln!(stdout, "{response}").expect("stdout should be writable");
    stdout.flush().expect("stdout should flush");
}
