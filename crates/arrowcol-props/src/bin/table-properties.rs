use std::error::Error;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = std::env::args().skip(1);
    let path = PathBuf::from(args.next().ok_or("expected documentation output path")?);
    let check = match args.next().as_deref() {
        None => false,
        Some("--check") => true,
        Some(_) => return Err("expected --check or no second argument".into()),
    };
    if args.next().is_some() {
        return Err("unexpected extra argument".into());
    }
    let content = arrowcol_props::documentation();
    if check {
        if std::fs::read_to_string(&path)? != content {
            return Err("table-property documentation is stale; run nx run arrowcol-props:generate-docs".into());
        }
    } else {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, content)?;
    }
    Ok(())
}
