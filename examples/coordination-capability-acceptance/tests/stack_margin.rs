//! The acceptance walk fits in half a default thread stack (issue #162).
//!
//! The walk is the deepest single chain of polls in the repository: a
//! dispatch pass invokes a handoff, the handoff executor calls the
//! in-process A2A service, the service applies a command to the task
//! entity, and the entity's choreography host writes its record. In a debug
//! build that chain once needed all but 64 KiB of a 2 MiB thread stack, and
//! one more layer on it overflowed a test that had nothing to do with the
//! change that added it.
//!
//! This test runs the same walk on a thread with half that. A test thread
//! and a Tokio worker thread both get 2 MiB by default, so a walk that
//! passes here has a default stack's other half as its margin, and a change
//! that eats the margin fails here while there is still room.
//!
//! # When this test fails
//!
//! It does not fail with an assertion. The thread overflows its stack and
//! the process aborts, naming the thread:
//!
//! ```text
//! thread 'the-acceptance-walk-on-half-a-default-stack' has overflowed its stack
//! ```
//!
//! That is why it is alone in this file: the abort takes the test binary
//! with it.
//!
//! A debug build keeps a stack slot for every future a function awaits, and
//! a future holds every future below it inline. So a function's frame is
//! about the sum of the futures it awaits, and one large future low in a
//! chain is paid for at every await site of every function above it. What
//! grew is almost always one future. To find it, print the sizes:
//!
//! ```sh
//! RUSTC_BOOTSTRAP=1 RUSTFLAGS=-Zprint-type-sizes CARGO_TARGET_DIR=/tmp/sizes \
//!   cargo build -p rakka-example-coordination-capability-acceptance --lib \
//!   | grep 'type: `{async' | sort -t: -k3 -n -r | head -40
//! ```
//!
//! and compare with main. The cure is to box the future where it is made
//! (`Box::pin(...).await`), at the lowest function that holds it, so that
//! nothing above it carries it inline. Do not raise the stack here.

use std::thread;

use rakka_example_coordination_capability_acceptance::report::EXPECTED_TRANSCRIPT;
use rakka_example_coordination_capability_acceptance::run_acceptance;

/// Half of the 2 MiB a test thread or a Tokio worker thread gets by default.
const HALF_A_DEFAULT_STACK: usize = 1024 * 1024;

#[test]
fn the_acceptance_walk_fits_in_half_a_default_thread_stack() {
    let walk = thread::Builder::new()
        .name("the-acceptance-walk-on-half-a-default-stack".to_string())
        .stack_size(HALF_A_DEFAULT_STACK)
        .spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("the runtime builds")
                .block_on(run_acceptance())
        })
        .expect("the thread starts");

    let report = walk.join().expect("the walk does not panic");

    assert_eq!(
        report.lines,
        EXPECTED_TRANSCRIPT
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        "the walk on a smaller stack is the same walk"
    );
}
