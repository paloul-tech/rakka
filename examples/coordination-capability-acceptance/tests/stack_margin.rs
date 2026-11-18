//! The acceptance walk fits in half a default thread stack (issue #162).
//!
//! The walk nests polls about twenty deep: a dispatch pass invokes a
//! handoff, the handoff executor calls the in-process A2A service, the
//! service applies a command to the task entity, and the entity's
//! choreography host writes its record. In a debug build that chain once
//! ran within 100 KiB of a 2 MiB thread stack, and one more level of
//! nesting on it overflowed a test that had nothing to do with the change
//! that added the level.
//!
//! This test runs the same walk on a thread with half that. A test thread
//! and a Tokio worker thread both get 2 MiB by default, so a walk that
//! passes here has a default stack's other half as its margin, and a change
//! that eats the margin fails here while there is still room.
//!
//! # What it holds, and what it does not
//!
//! One line, for one path. The walk needs about 780 KiB in a debug build,
//! so it can grow by about 240 KiB before this test fails, and no smaller
//! growth shows here. A path the walk does not take is not held at all.
//! When this was written six of `rakka-agent`'s test binaries needed more
//! stack than the walk does, the most of them 1,064 KiB
//! (`dispatch_failure_outcomes`), and nothing holds any of them. The
//! measurements are in
//! `docs/superpowers/specs/2026-11-17-dispatch-walk-stack-margin-design.md`.
//!
//! # When this test fails
//!
//! It does not fail with an assertion. The thread overflows its stack and
//! the process aborts, naming the thread and its id:
//!
//! ```text
//! thread 'the-acceptance-walk-on-half-a-default-stack' (4242) has overflowed its stack
//! fatal runtime error: stack overflow, aborting
//! ```
//!
//! That is why it is alone in this file: the abort takes the test binary
//! with it.
//!
//! A debug build keeps stack for every future a function awaits, about
//! twice the future's size, and a future holds every future below it
//! inline. So a large future low in a chain is paid for at every await of
//! every function above it, and a function added to the chain pays again
//! for what it awaits. To see which it was, print the size of every future:
//!
//! ```sh
//! RUSTC_BOOTSTRAP=1 RUSTFLAGS=-Zprint-type-sizes cargo build \
//!   -p rakka-example-coordination-capability-acceptance --lib \
//!   --target-dir /tmp/rakka-type-sizes > /tmp/rakka-type-sizes.log 2>&1
//! grep 'type: `{async' /tmp/rakka-type-sizes.log \
//!   | sed -E 's/^.*type: `(.*)`: ([0-9]+) bytes.*$/\2 \1/' \
//!   | sort -nr | head -40
//! ```
//!
//! A crate's sizes are printed when it compiles, so give the build a target
//! directory that does not exist yet: a second build into the same one
//! prints only what it rebuilds. Do the same on main and compare the lists.
//!
//! The cure is to box the future where it is made (`Box::pin(...).await`),
//! at a function every caller goes through, so that nothing above it
//! carries it inline. Do not raise the stack here.

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
