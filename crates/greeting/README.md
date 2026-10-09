# greeting

`greeting` is a reusable library crate under `crates/`. Library crates may be
shared by other libraries and by the independent applications under `bin/`.

The crate validates a name and returns a formatted greeting:

```rust
fn main() -> Result<(), greeting::GreetingError> {
    let message = greeting::greet("Ferris")?;
    assert_eq!(message, "Hello, Ferris!");
    Ok(())
}
```

Run its complete Nx verification target from the workspace root:

```bash
npx nx run greeting:verify
```

The package-scoped Cargo test command is:

```bash
cargo test --locked --package greeting
```
