//! Plain-text formatting for terminal output.

/// Human-readable binary size, such as `412.3 KiB`.
pub(crate) fn bytes(value: u64) -> String {
    const UNITS: [&str; 4] = ["KiB", "MiB", "GiB", "TiB"];
    if value < 1024 {
        return format!("{value} B");
    }
    #[allow(clippy::cast_precision_loss)]
    let mut scaled = value as f64 / 1024.0;
    let mut unit = 0;
    while scaled >= 1024.0 && unit + 1 < UNITS.len() {
        scaled /= 1024.0;
        unit += 1;
    }
    format!("{scaled:.1} {}", UNITS[unit])
}

/// Integer with thousands separators, such as `3,912`.
pub(crate) fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(digit);
    }
    grouped
}

/// Short form of a 32-byte schema fingerprint, such as `sha256:5e2a4c1f`.
pub(crate) fn fingerprint(bytes: &[u8]) -> String {
    let prefix = bytes
        .iter()
        .take(4)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("sha256:{prefix}")
}

/// Column alignment in a [`Table`].
#[derive(Clone, Copy)]
pub(crate) enum Align {
    Left,
    Right,
}

/// Left- or right-aligned text columns separated by two spaces.
pub(crate) struct Table {
    headers: Vec<(&'static str, Align)>,
    rows: Vec<Vec<String>>,
}

impl Table {
    pub(crate) fn new(headers: &[(&'static str, Align)]) -> Self {
        Self {
            headers: headers.to_vec(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn row(&mut self, cells: Vec<String>) {
        self.rows.push(cells);
    }

    /// Renders the table with a two-space indent.
    pub(crate) fn render(&self) -> String {
        let mut widths = self
            .headers
            .iter()
            .map(|(name, _)| name.chars().count())
            .collect::<Vec<_>>();
        for row in &self.rows {
            for (width, cell) in widths.iter_mut().zip(row) {
                *width = (*width).max(cell.chars().count());
            }
        }
        let line = |cells: Vec<&str>| {
            let text = cells
                .iter()
                .zip(&self.headers)
                .zip(&widths)
                .map(|((cell, (_, align)), width)| match align {
                    Align::Left => format!("{cell:<width$}"),
                    Align::Right => format!("{cell:>width$}"),
                })
                .collect::<Vec<_>>()
                .join("  ");
            format!("  {}", text.trim_end())
        };
        let mut lines = vec![line(self.headers.iter().map(|(name, _)| *name).collect())];
        lines.extend(
            self.rows
                .iter()
                .map(|row| line(row.iter().map(String::as_str).collect())),
        );
        lines.join("\n")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_and_counts_are_human_readable() {
        assert_eq!(bytes(512), "512 B");
        assert_eq!(bytes(422_195), "412.3 KiB");
        assert_eq!(bytes(4 * 1024 * 1024 * 1024), "4.0 GiB");
        assert_eq!(count(0), "0");
        assert_eq!(count(999), "999");
        assert_eq!(count(3_912), "3,912");
        assert_eq!(count(1_234_567), "1,234,567");
        assert_eq!(fingerprint(&[0x5e, 0x2a, 0x4c, 0x1f, 0xff]), "sha256:5e2a4c1f");
    }

    #[test]
    fn tables_align_columns() {
        let mut table = Table::new(&[("name", Align::Left), ("rows", Align::Right)]);
        table.row(vec!["a".to_owned(), "1".to_owned()]);
        table.row(vec!["longer".to_owned(), "1,024".to_owned()]);
        assert_eq!(table.render(), "  name     rows\n  a           1\n  longer  1,024");
    }
}
