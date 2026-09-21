//! Change detection for the paths a job depends on.
//!
//! Not integrity: these hashes answer "is this the same thing I submitted?"
//! for a store only its owner writes. They are fast and non-cryptographic,
//! and nothing here defends against someone who can rewrite the store.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Read;
use std::path::Path;

use crate::error::{Error, Result};

/// A file's hash, from its contents, streamed so a large binary does not
/// land in memory.
pub(crate) fn file_hash(path: &Path) -> Result<String> {
    let mut file = std::fs::File::open(path).map_err(|e| Error::io(path, e))?;
    let mut hasher = DefaultHasher::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = file.read(&mut buf).map_err(|e| Error::io(path, e))?;
        if n == 0 {
            break;
        }
        buf[..n].hash(&mut hasher);
    }
    Ok(format!("{:016x}", hasher.finish()))
}

/// A directory's hash, from the shape of the tree rather than the bytes in
/// it: every entry's relative path, length and modification time, in sorted
/// order.
///
/// That catches an edit, an addition, a deletion and a rename, and costs one
/// `stat` per file rather than a full read of a dataset. The tradeoff is
/// that `touch` alone looks like a change — acceptable where the answer is a
/// warning, which is why a directory defaults to [`OnChange::Warn`].
///
/// [`OnChange::Warn`]: crate::OnChange::Warn
pub(crate) fn dir_hash(path: &Path) -> Result<String> {
    let mut entries = Vec::new();
    collect(path, path, &mut entries)?;
    entries.sort();

    let mut hasher = DefaultHasher::new();
    for entry in &entries {
        entry.hash(&mut hasher);
    }
    Ok(format!("{:016x}", hasher.finish()))
}

/// Hash whatever is at `path`, file or directory.
pub(crate) fn path_hash(path: &Path) -> Result<String> {
    let meta = std::fs::metadata(path).map_err(|e| Error::io(path, e))?;
    if meta.is_dir() {
        dir_hash(path)
    } else {
        file_hash(path)
    }
}

type Entry = (String, u64, i64);

fn collect(root: &Path, dir: &Path, out: &mut Vec<Entry>) -> Result<()> {
    let read = std::fs::read_dir(dir).map_err(|e| Error::io(dir, e))?;
    for entry in read {
        let entry = entry.map_err(|e| Error::io(dir, e))?;
        let path = entry.path();
        let meta = entry.metadata().map_err(|e| Error::io(&path, e))?;
        if meta.is_dir() {
            collect(root, &path, out)?;
            continue;
        }
        let rel = path
            .strip_prefix(root)
            .unwrap_or(&path)
            .to_string_lossy()
            .into_owned();
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        out.push((rel, meta.len(), mtime));
    }
    Ok(())
}
