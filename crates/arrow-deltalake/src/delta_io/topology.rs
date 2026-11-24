use anyhow::{Context, Result, ensure};
use std::{collections::BTreeMap, fs, path::Path};

pub(super) const CPU_ROOT: &str = "/sys/devices/system/cpu";

fn attribute(root: &Path, cpu: usize, key: &str) -> Result<String> {
    let path = root.join(format!("cpu{cpu}/topology/{key}"));
    fs::read_to_string(&path).with_context(|| format!("read {}", path.display()))
}

/// Read the physical package and core IDs for one logical CPU.
///
/// # Errors
/// Returns an error for missing, unreadable or invalid topology attributes.
pub(super) fn core(root: &Path, cpu: usize) -> Result<(usize, usize)> {
    let id = |key| -> Result<usize> {
        attribute(root, cpu, key)?
            .trim()
            .parse()
            .with_context(|| format!("invalid {key} for CPU {cpu}"))
    };
    Ok((id("physical_package_id")?, id("core_id")?))
}

fn allowed_siblings(list: &str, allowed: &[usize]) -> Result<Vec<usize>> {
    let mut siblings = Vec::new();
    let mut previous = None;
    for item in list.trim().split(',') {
        let (first, last) = item.split_once('-').unwrap_or((item, item));
        let (first, last) = (first.parse::<usize>()?, last.parse::<usize>()?);
        ensure!(
            first <= last && previous.is_none_or(|end| end < first),
            "invalid sibling ranges: {list}"
        );
        previous = Some(last);
        siblings.extend_from_slice(
            &allowed[allowed.partition_point(|id| *id < first)..allowed.partition_point(|id| *id <= last)],
        );
    }
    Ok(siblings)
}

/// Rank the sorted effective CPU mask by allowed sibling rank, then logical ID.
///
/// # Errors
/// Returns an error if sysfs topology is unavailable or contradicts the sibling lists.
pub(super) fn physical_first(root: &Path, allowed: &[usize]) -> Result<Vec<usize>> {
    let mut cores = BTreeMap::<_, Vec<_>>::new();
    let mut observed = Vec::new();
    for &cpu in allowed {
        let key = core(root, cpu)?;
        cores.entry(key).or_default().push(cpu);
        let list = attribute(root, cpu, "thread_siblings_list")?;
        observed.push((cpu, key, allowed_siblings(&list, allowed)?));
    }
    for (cpu, key, siblings) in observed {
        ensure!(
            siblings == cores[&key],
            "CPU {cpu} sibling list disagrees with allowed package/core topology"
        );
    }
    let mut ranked = Vec::with_capacity(allowed.len());
    for siblings in cores.values() {
        ranked.extend(siblings.iter().copied().enumerate());
    }
    ranked.sort_unstable(); // One logical CPU per core before the next sibling rank; tie-break by CPU ID.
    Ok(ranked.into_iter().map(|(_, cpu)| cpu).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restricted_mask_cross_package_and_inconsistent_sysfs() -> Result<()> {
        let fixture = tempfile::tempdir()?;
        let root = fixture.path();
        for (cpu, package, core, siblings) in [(1, 0, 7, "0-2"), (2, 0, 7, "0-2"), (8, 1, 7, "8,16")] {
            let dir = root.join(format!("cpu{cpu}/topology"));
            fs::create_dir_all(&dir)?;
            for (key, value) in [
                ("physical_package_id", package.to_string()),
                ("core_id", core.to_string()),
                ("thread_siblings_list", siblings.into()),
            ] {
                fs::write(dir.join(key), value)?;
            }
        }
        let allowed = [1, 2, 8];
        assert_eq!(physical_first(root, &allowed)?, [1, 8, 2]);
        let file = root.join("cpu2/topology/thread_siblings_list");
        for invalid in ["2", "1,2,8", "2-1", "", "bad"] {
            fs::write(&file, invalid)?;
            assert!(physical_first(root, &allowed).is_err(), "{invalid}");
        }
        fs::remove_file(file)?;
        assert!(physical_first(root, &allowed).is_err());
        Ok(())
    }
}
