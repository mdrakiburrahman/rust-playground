use assert_cmd::Command;
use predicates::prelude::*;

fn hello_world() -> Command {
    Command::new(env!("CARGO_BIN_EXE_hello-world"))
}

#[test]
fn greets_the_default_name() {
    hello_world().assert().success().stdout("Hello, World!\n");
}

#[test]
fn greets_the_requested_name() {
    hello_world()
        .args(["--name", "Ferris"])
        .assert()
        .success()
        .stdout("Hello, Ferris!\n");
}

#[test]
fn rejects_a_blank_name() {
    hello_world()
        .args(["--name", "   "])
        .assert()
        .failure()
        .stderr(predicate::str::contains("name must not be empty"));
}
