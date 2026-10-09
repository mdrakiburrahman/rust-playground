//! Validated greeting creation for workspace applications.
//!
//! The [`greet`] function provides the small reusable boundary between input
//! validation and presentation.

use thiserror::Error;

/// An error returned while creating a greeting.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum GreetingError {
    /// The supplied name was empty or contained only whitespace.
    #[error("name must not be empty")]
    EmptyName,
}

/// Creates a friendly greeting for `name`.
///
/// Leading and trailing whitespace is removed before the greeting is built.
///
/// # Examples
///
/// ```
/// let message = greeting::greet("Ferris")?;
///
/// assert_eq!(message, "Hello, Ferris!");
/// # Ok::<(), greeting::GreetingError>(())
/// ```
///
/// # Errors
///
/// Returns [`GreetingError::EmptyName`] when `name` is empty or contains only
/// whitespace.
pub fn greet(name: &str) -> Result<String, GreetingError> {
    let name = name.trim();

    if name.is_empty() {
        return Err(GreetingError::EmptyName);
    }

    Ok(format!("Hello, {name}!"))
}

#[cfg(test)]
mod tests {
    use super::{GreetingError, greet};

    #[test]
    fn greets_a_name() {
        assert_eq!(greet("Ferris").as_deref(), Ok("Hello, Ferris!"));
    }

    #[test]
    fn trims_surrounding_whitespace() {
        assert_eq!(greet("  Rust  ").as_deref(), Ok("Hello, Rust!"));
    }

    #[test]
    fn rejects_an_empty_name() {
        assert_eq!(greet(""), Err(GreetingError::EmptyName));
        assert_eq!(greet(" \t\n"), Err(GreetingError::EmptyName));
    }
}
