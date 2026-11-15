//! Alone in its process: what an allocator keeps depends on everything the process freed before.
#![cfg(all(target_os = "linux", target_env = "gnu"))]

use anyhow::Context;

const MIB: usize = 1024 * 1024;

#[test]
fn the_memory_of_a_large_allocation_is_returned_when_it_is_freed() -> anyhow::Result<()> {
    durable_actors::host::return_large_allocations_when_freed()?;
    // On its own, glibc keeps what is freed from here on if it is smaller than this.
    drop(payload(16 * MIB));
    let before = resident_bytes()?;

    drop(payload(8 * MIB));

    assert!(resident_bytes()? < before + MIB);
    Ok(())
}

fn payload(bytes: usize) -> Vec<u8> {
    std::hint::black_box(vec![1; bytes])
}

fn resident_bytes() -> anyhow::Result<usize> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let kilobytes = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))
        .and_then(|value| value.split_whitespace().next())
        .context("no resident size in /proc/self/status")?;
    Ok(kilobytes.parse::<usize>()? * 1024)
}
