use {
    bzip2::bufread::BzDecoder,
    log::*,
    // rand::{thread_rng, Rng},
    solana_genesis_config::{GenesisConfig, DEFAULT_GENESIS_ARCHIVE, DEFAULT_GENESIS_FILE},
    std::{
        // collections::HashMap,
        fs::{self, File},
        io::{BufReader, Read},
        path::{
            Component::{self, CurDir, Normal},
            Path, PathBuf,
        },
        time::Instant,
    },
    tar::{
        Archive,
        EntryType::{Directory, GNUSparse, Regular},
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum UnpackError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Archive error: {0}")]
    Archive(String),
}

const MAX_GENESIS_ARCHIVE_UNPACKED_COUNT: u64 = 100;

pub type Result<T> = std::result::Result<T, UnpackError>;

#[derive(Error, Debug)]
pub enum OpenGenesisConfigError {
    #[error("unpack error: {0}")]
    Unpack(#[from] UnpackError),
    #[error("Genesis load error: {0}")]
    Load(#[from] std::io::Error),
}

pub fn open_genesis_config(
    ledger_path: &Path,
    max_genesis_archive_unpacked_size: u64,
) -> std::result::Result<GenesisConfig, OpenGenesisConfigError> {
    match GenesisConfig::load(ledger_path) {
        Ok(genesis_config) => Ok(genesis_config),
        Err(load_err) => {
            warn!(
                "Failed to load genesis_config at {ledger_path:?}: {load_err}. \
                Will attempt to unpack genesis archive and then retry loading."
            );

            let genesis_package = ledger_path.join(DEFAULT_GENESIS_ARCHIVE);
            unpack_genesis_archive(
                &genesis_package,
                ledger_path,
                max_genesis_archive_unpacked_size,
            )?;
            GenesisConfig::load(ledger_path).map_err(OpenGenesisConfigError::Load)
        }
    }
}

pub fn unpack_genesis_archive(
    archive_filename: &Path,
    destination_dir: &Path,
    max_genesis_archive_unpacked_size: u64,
) -> std::result::Result<(), UnpackError> {
    info!("Extracting {:?}...", archive_filename);
    let extract_start = Instant::now();

    fs::create_dir_all(destination_dir)?;
    let tar_bz2 = File::open(archive_filename)?;
    let tar = BzDecoder::new(BufReader::new(tar_bz2));
    let mut archive = Archive::new(tar);
    unpack_genesis(
        &mut archive,
        destination_dir,
        max_genesis_archive_unpacked_size,
    )?;
    info!(
        "Extracted {:?} in {:?}",
        archive_filename,
        Instant::now().duration_since(extract_start)
    );
    Ok(())
}

fn unpack_genesis<A: Read>(
    archive: &mut Archive<A>,
    unpack_dir: &Path,
    max_genesis_archive_unpacked_size: u64,
) -> Result<()> {
    unpack_archive(
        archive,
        max_genesis_archive_unpacked_size,
        max_genesis_archive_unpacked_size,
        MAX_GENESIS_ARCHIVE_UNPACKED_COUNT,
        |p, k| is_valid_genesis_archive_entry(unpack_dir, p, k),
        |_| {},
    )
}

fn is_valid_genesis_archive_entry<'a>(
    unpack_dir: &'a Path,
    parts: &[&str],
    kind: tar::EntryType,
) -> UnpackPath<'a> {
    trace!("validating: {:?} {:?}", parts, kind);
    #[allow(clippy::match_like_matches_macro)]
    match (parts, kind) {
        ([DEFAULT_GENESIS_FILE], GNUSparse) => UnpackPath::Valid(unpack_dir),
        ([DEFAULT_GENESIS_FILE], Regular) => UnpackPath::Valid(unpack_dir),
        (["rocksdb"], Directory) => UnpackPath::Ignore,
        (["rocksdb", _], GNUSparse) => UnpackPath::Ignore,
        (["rocksdb", _], Regular) => UnpackPath::Ignore,
        (["rocksdb_fifo"], Directory) => UnpackPath::Ignore,
        (["rocksdb_fifo", _], GNUSparse) => UnpackPath::Ignore,
        (["rocksdb_fifo", _], Regular) => UnpackPath::Ignore,
        _ => UnpackPath::Invalid,
    }
}

fn checked_total_size_sum(total_size: u64, entry_size: u64, limit_size: u64) -> Result<u64> {
    trace!(
        "checked_total_size_sum: {} + {} < {}",
        total_size,
        entry_size,
        limit_size,
    );
    let total_size = total_size.saturating_add(entry_size);
    if total_size > limit_size {
        return Err(UnpackError::Archive(format!(
            "too large archive: {total_size} than limit: {limit_size}",
        )));
    }
    Ok(total_size)
}

fn checked_total_count_increment(total_count: u64, limit_count: u64) -> Result<u64> {
    let total_count = total_count + 1;
    if total_count > limit_count {
        return Err(UnpackError::Archive(format!(
            "too many files in snapshot: {total_count:?}"
        )));
    }
    Ok(total_count)
}

fn check_unpack_result(unpack_result: bool, path: String) -> Result<()> {
    if !unpack_result {
        return Err(UnpackError::Archive(format!("failed to unpack: {path:?}")));
    }
    Ok(())
}

// return Err on file system error
// return Some(path) if path is good
// return None if we should skip this file
fn sanitize_path(entry_path: &Path, dst: &Path) -> Result<Option<PathBuf>> {
    // We cannot call unpack_in because it errors if we try to use 2 account paths.
    // So, this code is borrowed from unpack_in
    // ref: https://docs.rs/tar/*/tar/struct.Entry.html#method.unpack_in
    let mut file_dst = dst.to_path_buf();
    const SKIP: Result<Option<PathBuf>> = Ok(None);
    {
        let path = entry_path;
        for part in path.components() {
            match part {
                // Leading '/' characters, root paths, and '.'
                // components are just ignored and treated as "empty
                // components"
                Component::Prefix(..) | Component::RootDir | Component::CurDir => continue,

                // If any part of the filename is '..', then skip over
                // unpacking the file to prevent directory traversal
                // security issues.  See, e.g.: CVE-2001-1267,
                // CVE-2002-0399, CVE-2005-1918, CVE-2007-4131
                Component::ParentDir => return SKIP,

                Component::Normal(part) => file_dst.push(part),
            }
        }
    }

    // Skip cases where only slashes or '.' parts were seen, because
    // this is effectively an empty filename.
    if *dst == *file_dst {
        return SKIP;
    }

    // Skip entries without a parent (i.e. outside of FS root)
    let Some(parent) = file_dst.parent() else {
        return SKIP;
    };

    fs::create_dir_all(parent)?;

    // Here we are different than untar_in. The code for tar::unpack_in internally calling unpack is a little different.
    // ignore return value here
    validate_inside_dst(dst, parent)?;
    let target = parent.join(entry_path.file_name().unwrap());

    Ok(Some(target))
}

#[derive(Debug, PartialEq, Eq)]
pub enum UnpackPath<'a> {
    Valid(&'a Path),
    Ignore,
    Invalid,
}

fn unpack_archive<'a, A, C, D>(
    archive: &mut Archive<A>,
    apparent_limit_size: u64,
    actual_limit_size: u64,
    limit_count: u64,
    mut entry_checker: C, // checks if entry is valid
    entry_processor: D,   // processes entry after setting permissions
) -> Result<()>
where
    A: Read,
    C: FnMut(&[&str], tar::EntryType) -> UnpackPath<'a>,
    D: Fn(PathBuf),
{
    let mut apparent_total_size: u64 = 0;
    let mut actual_total_size: u64 = 0;
    let mut total_count: u64 = 0;

    let mut total_entries = 0;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let path_str = path.display().to_string();

        // Although the `tar` crate safely skips at the actual unpacking, fail
        // first by ourselves when there are odd paths like including `..` or /
        // for our clearer pattern matching reasoning:
        //   https://docs.rs/tar/0.4.26/src/tar/entry.rs.html#371
        let parts = path
            .components()
            .map(|p| match p {
                CurDir => Ok("."),
                Normal(c) => c.to_str().ok_or(()),
                _ => Err(()), // Prefix (for Windows) and RootDir are forbidden
            })
            .collect::<std::result::Result<Vec<_>, _>>();

        // Reject old-style BSD directory entries that aren't explicitly tagged as directories
        let legacy_dir_entry =
            entry.header().as_ustar().is_none() && entry.path_bytes().ends_with(b"/");
        let kind = entry.header().entry_type();
        let reject_legacy_dir_entry = legacy_dir_entry && (kind != Directory);
        let (Ok(parts), false) = (parts, reject_legacy_dir_entry) else {
            return Err(UnpackError::Archive(format!(
                "invalid path found: {path_str:?}"
            )));
        };

        let unpack_dir = match entry_checker(parts.as_slice(), kind) {
            UnpackPath::Invalid => {
                return Err(UnpackError::Archive(format!(
                    "extra entry found: {:?} {:?}",
                    path_str,
                    entry.header().entry_type(),
                )));
            }
            UnpackPath::Ignore => {
                continue;
            }
            UnpackPath::Valid(unpack_dir) => unpack_dir,
        };

        apparent_total_size = checked_total_size_sum(
            apparent_total_size,
            entry.header().size()?,
            apparent_limit_size,
        )?;
        actual_total_size = checked_total_size_sum(
            actual_total_size,
            entry.header().entry_size()?,
            actual_limit_size,
        )?;
        total_count = checked_total_count_increment(total_count, limit_count)?;

        let account_filename = match parts.as_slice() {
            ["accounts", account_filename] => Some(PathBuf::from(account_filename)),
            _ => None,
        };
        let entry_path = if let Some(account) = account_filename {
            // Special case account files. We're unpacking an account entry inside one of the
            // account_paths returned by `entry_checker`. We want to unpack into
            // account_path/<account> instead of account_path/accounts/<account> so we strip the
            // accounts/ prefix.
            sanitize_path(&account, unpack_dir)
        } else {
            sanitize_path(&path, unpack_dir)
        }?; // ? handles file system errors
        let Some(entry_path) = entry_path else {
            continue; // skip it
        };

        let unpack = entry.unpack(&entry_path);
        check_unpack_result(unpack.map(|_unpack| true)?, path_str)?;

        // Sanitize permissions.
        let mode = match entry.header().entry_type() {
            GNUSparse | Regular => 0o644,
            _ => 0o755,
        };
        set_perms(&entry_path, mode)?;

        // Process entry after setting permissions
        entry_processor(entry_path);

        total_entries += 1;
    }
    info!("unpacked {} entries total", total_entries);

    return Ok(());

    #[cfg(unix)]
    fn set_perms(dst: &Path, mode: u32) -> std::io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let perm = fs::Permissions::from_mode(mode as _);
        fs::set_permissions(dst, perm)
    }

    #[cfg(windows)]
    fn set_perms(dst: &Path, _mode: u32) -> std::io::Result<()> {
        let mut perm = fs::metadata(dst)?.permissions();
        // This is OK for Windows, but clippy doesn't realize we're doing this
        // only on Windows.
        #[allow(clippy::permissions_set_readonly_false)]
        perm.set_readonly(false);
        fs::set_permissions(dst, perm)
    }
}

// copied from:
// https://github.com/alexcrichton/tar-rs/blob/d90a02f582c03dfa0fd11c78d608d0974625ae5d/src/entry.rs#L781
fn validate_inside_dst(dst: &Path, file_dst: &Path) -> Result<PathBuf> {
    // Abort if target (canonical) parent is outside of `dst`
    let canon_parent = file_dst.canonicalize().map_err(|err| {
        UnpackError::Archive(format!(
            "{} while canonicalizing {}",
            err,
            file_dst.display()
        ))
    })?;
    let canon_target = dst.canonicalize().map_err(|err| {
        UnpackError::Archive(format!("{} while canonicalizing {}", err, dst.display()))
    })?;
    if !canon_parent.starts_with(&canon_target) {
        return Err(UnpackError::Archive(format!(
            "trying to unpack outside of destination path: {}",
            canon_target.display()
        )));
    }
    Ok(canon_target)
}