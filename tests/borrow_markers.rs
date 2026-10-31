// Every lifetime marker in the crate's types is a `ScopedBorrow`.
//
// A borrow recorded only as `PhantomData<&'a ()>` has no drop glue that names `'a`, so the
// borrow checker ends it at the value's last use. A value that still holds the table's pages or
// a transaction guard at that point, as `Range`, `MultimapRange` and `Cursor` once did, lets the
// table be mutated, or its transaction committed, under it. `ScopedBorrow<'a>` (see
// src/tree_store/page_store/base.rs) keeps the borrow until the value is dropped. This test greps
// for every other `PhantomData` that names a lifetime: the two there are by design are listed in
// `ALLOWED`, with the reason, and any other fails.

// The wasi runner maps only /tmp, so the sources cannot be read there
#![cfg(not(target_os = "wasi"))]

use std::fs;
use std::path::{Path, PathBuf};

// The file, and the line without its indentation or a trailing comment
const ALLOWED: &[(&str, &str)] = &[
    // The marker itself, wrapped in the Drop impl that keeps the borrow
    (
        "src/tree_store/page_store/base.rs",
        "pub(crate) struct ScopedBorrow<'a>(PhantomData<&'a ()>);",
    ),
    // The stable iterators keep the last-use borrow until redb 5
    (
        "src/table.rs",
        "pub(crate) type CompatBorrow<'a> = PhantomData<&'a ()>;",
    ),
];

#[test]
fn lifetime_markers_are_scoped_borrows() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut files = vec![];
    rust_files(&root.join("src"), &mut files);
    files.sort();
    let mut unlisted = vec![];
    for path in &files {
        let file = path
            .strip_prefix(root)
            .unwrap()
            .to_string_lossy()
            .replace('\\', "/");
        let source = fs::read_to_string(path).unwrap();
        for (index, line) in source.lines().enumerate() {
            let code = line.split("//").next().unwrap().trim();
            let names_lifetime = code
                .find("PhantomData<")
                .is_some_and(|start| code[start..].contains('\''));
            if names_lifetime && !ALLOWED.iter().any(|&(f, l)| f == file && l == code) {
                unlisted.push(format!("{file}:{}: {code}", index + 1));
            }
        }
    }
    assert!(
        unlisted.is_empty(),
        "lifetime markers that are not a `ScopedBorrow`. A borrow is recorded through \
         `ScopedBorrow<'a>`, so that it lasts until the value is dropped; a bare `PhantomData` \
         naming a lifetime is allowed only where `ALLOWED` in {} lists it, with the reason:\n{}",
        file!(),
        unlisted.join("\n")
    );
}

fn rust_files(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            rust_files(&path, files);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            files.push(path);
        }
    }
}
