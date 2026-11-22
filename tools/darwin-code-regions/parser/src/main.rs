use std::io::{self, Read};
use syn::visit::Visit;

const MAX_SOURCE_BYTES: usize = 1_048_576;

#[derive(Default)]
struct BodyCheck {
    unsupported: bool,
}

impl<'ast> Visit<'ast> for BodyCheck {
    fn visit_item(&mut self, _: &'ast syn::Item) {
        self.unsupported = true;
    }

    fn visit_attribute(&mut self, _: &'ast syn::Attribute) {
        self.unsupported = true;
    }

    fn visit_macro(&mut self, _: &'ast syn::Macro) {
        self.unsupported = true;
    }
}

fn finish_body(source: &str) -> Result<std::ops::Range<usize>, String> {
    if source.starts_with('\u{feff}') || source.starts_with("#!") && !source.starts_with("#![") {
        return Err("BOM/shebang source is outside this lane".into());
    }
    let file = syn::parse_file(source).map_err(|error| format!("invalid Rust source: {error}"))?;
    let mut matches = Vec::new();
    for item in &file.items {
        let syn::Item::Impl(item) = item else { continue };
        let syn::Type::Path(ty) = &*item.self_ty else { continue };
        if item.trait_.is_some()
            || ty.qself.is_some()
            || ty.path.leading_colon.is_some()
            || ty.path.segments.len() != 1
            || ty.path.segments[0].ident != "FileSortBuffer"
            || !ty.path.segments[0].arguments.is_none()
        {
            continue;
        }
        for member in &item.items {
            if let syn::ImplItem::Fn(method) = member
                && method.sig.ident == "finish"
            {
                matches.push(method);
            }
        }
    }
    if matches.len() != 1 {
        return Err(format!(
            "expected exactly one root inherent FileSortBuffer::finish, found {}",
            matches.len()
        ));
    }
    let method = matches[0];
    if method
        .attrs
        .iter()
        .any(|attr| matches!(attr.style, syn::AttrStyle::Inner(_)))
    {
        return Err("inner method attributes are outside this lane".into());
    }
    let mut body_check = BodyCheck::default();
    body_check.visit_block(&method.block);
    if body_check.unsupported {
        return Err("nested items, attributes, and unexpanded macros are outside this lane".into());
    }
    let open = method.block.brace_token.span.open().byte_range();
    let close = method.block.brace_token.span.close().byte_range();
    if open.end != open.start + 1
        || close.end != close.start + 1
        || open.end > close.start
        || source.as_bytes().get(open.start) != Some(&b'{')
        || source.as_bytes().get(close.start) != Some(&b'}')
        || !source.is_char_boundary(open.end)
        || !source.is_char_boundary(close.start)
    {
        return Err("parser did not return valid source byte spans".into());
    }
    Ok(open.end..close.start)
}

fn run() -> Result<(), String> {
    if std::env::args_os().len() != 1 {
        return Err("parser takes source on stdin, not an item selector".into());
    }
    let mut bytes = Vec::new();
    io::stdin()
        .take((MAX_SOURCE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.is_empty() || bytes.len() > MAX_SOURCE_BYTES {
        return Err("source must contain 1..=1048576 bytes".into());
    }
    let source = std::str::from_utf8(&bytes).map_err(|error| format!("source is not UTF-8: {error}"))?;
    let body = finish_body(source)?;
    println!("{}:{}", body.start, body.end);
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
