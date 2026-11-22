use std::ffi::CString;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::Command;

const CREATE_RULESET: i64 = 444;
const ADD_RULE: i64 = 445;
const RESTRICT_SELF: i64 = 446;
const VERSION: u32 = 1;
const PATH_BENEATH: u32 = 1;
const O_PATH: i32 = 0o10000000;
const O_CLOEXEC: i32 = 0o2000000;
const PR_SET_NO_NEW_PRIVS: i32 = 38;
const EXECUTE: u64 = 1 << 0;
const READ_FILE: u64 = 1 << 2;
const READ_DIR: u64 = 1 << 3;
const READ: u64 = EXECUTE | READ_FILE | READ_DIR;
const ALL: u64 = (1 << 15) - 1;

#[repr(C)]
struct Ruleset {
    handled_access_fs: u64,
}

#[repr(C)]
struct PathBeneath {
    allowed_access: u64,
    parent_fd: i32,
}

extern "C" {
    fn syscall(number: i64, ...) -> i64;
    fn open(path: *const i8, flags: i32, ...) -> i32;
    fn close(fd: i32) -> i32;
    fn prctl(option: i32, ...) -> i32;
}

fn rule(ruleset: i32, path: &Path, access: u64) -> io::Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let path = CString::new(path.as_os_str().as_bytes())?;
    let fd = unsafe { open(path.as_ptr(), O_PATH | O_CLOEXEC) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let attr = PathBeneath {
        allowed_access: access,
        parent_fd: fd,
    };
    let result = unsafe { syscall(ADD_RULE, ruleset, PATH_BENEATH, &attr, 0) };
    unsafe { close(fd) };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    if args.len() != 3 || !cfg!(all(target_os = "linux", target_arch = "x86_64")) {
        return Err(io::Error::other(
            "usage: sandbox <binary> <active|idle> <arm>; Linux x86_64 only",
        ));
    }
    let abi = unsafe { syscall(CREATE_RULESET, 0, 0, VERSION) };
    if abi < 3 {
        return Err(io::Error::other(format!("Landlock ABI 3 required, found {abi}")));
    }
    let attr = Ruleset { handled_access_fs: ALL };
    let ruleset = unsafe { syscall(CREATE_RULESET, &attr, std::mem::size_of::<Ruleset>(), 0) };
    if ruleset < 0 {
        return Err(io::Error::last_os_error());
    }
    let ruleset = ruleset as i32;
    rule(ruleset, Path::new("/"), READ_DIR)?;
    for path in ["/usr", "/lib", "/lib64", "/proc"] {
        if Path::new(path).exists() {
            rule(ruleset, Path::new(path), READ)?;
        }
    }
    for path in [
        "/etc/ld.so.cache",
        "/dev/null",
        "/sys/fs/cgroup/cpu.max",
        "/sys/devices/system/cpu/online",
    ] {
        if Path::new(path).exists() {
            rule(ruleset, Path::new(path), READ_FILE)?;
        }
    }
    rule(ruleset, Path::new(&args[0]), EXECUTE | READ_FILE)?;
    let parent = Path::new(&args[2])
        .parent()
        .ok_or_else(|| io::Error::other("arm needs a parent"))?;
    rule(ruleset, parent, READ)?;
    rule(ruleset, Path::new(&args[2]), ALL)?;
    if unsafe { prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) } != 0 || unsafe { syscall(RESTRICT_SELF, ruleset, 0) } != 0 {
        return Err(io::Error::last_os_error());
    }
    unsafe { close(ruleset) };
    Err(Command::new(&args[0]).arg(&args[1]).arg(&args[2]).exec())
}
