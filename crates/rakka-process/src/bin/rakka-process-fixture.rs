#![forbid(unsafe_code)]

//! Child-process fixture executable for `rakka-process` integration tests.

use std::io::{BufRead, Read, Write};
use std::net::TcpListener;
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
        "fixture_one_shot_echo" => one_shot_echo(),
        "fixture_one_shot_sleeps" => one_shot_sleeps(),
        "fixture_one_shot_large_stdout" => one_shot_large_stdout(),
        "fixture_file_watch_success" => file_watch_success(),
        "fixture_process_entity_lifecycle" => process_entity_lifecycle(),
        "fixture_tcp_server" => tcp_server(),
        #[cfg(unix)]
        "fixture_unix_server" => unix_server(),
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

fn one_shot_echo() {
    let mut stdin = std::io::stdin();
    let mut input = String::new();
    stdin
        .read_to_string(&mut input)
        .expect("stdin should be readable");
    println!("stdout:{input}");
    eprintln!("stderr:{input}");
}

fn one_shot_sleeps() {
    std::thread::sleep(Duration::from_secs(5));
}

fn one_shot_large_stdout() {
    let chunk = vec![b'x'; 4096];
    std::io::stdout()
        .write_all(&chunk)
        .expect("stdout should be writable");
}

fn file_watch_success() {
    let input = std::fs::read_to_string("input.txt").expect("input.txt should be readable");
    std::fs::write("output.txt", format!("processed:{input}")).expect("output.txt should write");
    eprintln!("file-watch-ready");
    waits_for_stdin_eof();
}

fn process_entity_lifecycle() {
    let log_path = std::env::args()
        .nth(2)
        .expect("fixture_process_entity_lifecycle requires a log path");
    append_line(&log_path, "start");
    waits_for_stdin_eof();
    append_line(&log_path, "stop");
}

fn tcp_server() {
    let port = std::env::args()
        .nth(2)
        .expect("fixture_tcp_server requires a port")
        .parse::<u16>()
        .expect("port should parse");
    let _listener = TcpListener::bind(("127.0.0.1", port)).expect("tcp listener should bind");
    waits_for_stdin_eof();
}

#[cfg(unix)]
fn unix_server() {
    let path = std::env::args()
        .nth(2)
        .expect("fixture_unix_server requires a path");
    let _removed = std::fs::remove_file(&path);
    let _listener =
        std::os::unix::net::UnixListener::bind(&path).expect("unix listener should bind");
    waits_for_stdin_eof();
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

fn append_line(path: &str, line: &str) {
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("log file should open");
    writeln!(file, "{line}").expect("log file should write");
}
