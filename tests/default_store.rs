//! The default store is resolved from the environment, so this lives in its
//! own test binary: one test, one process, no other thread racing on env.

use pekaren::{Queue, default_store_path};

#[test]
fn default_store_follows_the_environment() {
    let dir = tempfile::tempdir().unwrap();
    let home = dir.path().join("home");
    let explicit = dir.path().join("elsewhere");

    // SAFETY: this test binary holds exactly one test, so nothing else is
    // reading the environment while we write it.
    unsafe {
        std::env::set_var("HOME", &home);
        std::env::remove_var("PEKAREN_STORE");
    }
    assert_eq!(default_store_path(), home.join(".pekaren"));

    unsafe { std::env::set_var("PEKAREN_STORE", &explicit) }
    assert_eq!(default_store_path(), explicit);

    // And opening it puts a store there, creating the directory on the way.
    let q = Queue::options()
        .work_on_submit(false)
        .open_default()
        .unwrap();
    assert_eq!(q.path(), explicit);
    assert!(explicit.join("store.db").exists());

    // A tilde in the variable is expanded like any other store path.
    unsafe { std::env::set_var("PEKAREN_STORE", "~/scratch-queue") }
    assert_eq!(default_store_path(), home.join("scratch-queue"));
}
