#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MAPPED_FROM_BYTES: nix::libc::c_int = 128 * 1024;

/// Has glibc map each allocation of 128 KiB or more on its own, so that freeing it returns the
/// memory. Left alone, glibc stops mapping allocations smaller than the largest one freed so far,
/// up to 32 MiB, and what is freed after that stays in the heap of the thread that freed it: 60
/// messages of 12 MiB took a host that idles at 38 MiB to 149 MiB of its sandbox's 256.
pub fn return_large_allocations_when_freed() -> anyhow::Result<()> {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: sets an allocator tunable, which glibc allows at any time and from any thread.
        let set = unsafe { nix::libc::mallopt(nix::libc::M_MMAP_THRESHOLD, MAPPED_FROM_BYTES) };
        anyhow::ensure!(
            set == 1,
            "glibc refused to map allocations from {MAPPED_FROM_BYTES} bytes"
        );
    }
    Ok(())
}
