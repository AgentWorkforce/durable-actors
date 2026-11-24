/// CPU time this process has used so far, user and system, in milliseconds; `None` off Linux.
pub(crate) fn cpu_ms() -> Option<f64> {
    #[cfg(target_os = "linux")]
    {
        use nix::sys::resource::{UsageWho, getrusage};
        let usage = getrusage(UsageWho::RUSAGE_SELF).ok()?;
        let ms = |time: nix::sys::time::TimeVal| {
            time.tv_sec() as f64 * 1000.0 + time.tv_usec() as f64 / 1000.0
        };
        Some(ms(usage.user_time()) + ms(usage.system_time()))
    }
    #[cfg(not(target_os = "linux"))]
    None
}

pub(crate) struct Sample {
    pub cpu_ms: Option<f64>,
    pub peak_rss_bytes: Option<u64>,
}

pub(crate) async fn measure<T>(work: impl std::future::Future<Output = T>) -> (T, Sample) {
    let cpu = cpu_ms();
    let mut peak = resident_bytes();
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(work);
    let result = loop {
        tokio::select! {
            biased;
            value = &mut work => break value,
            _ = tick.tick() => peak = peak.max(resident_bytes()),
        }
    };
    (
        result,
        Sample {
            cpu_ms: cpu.zip(cpu_ms()).map(|(start, end)| end - start),
            peak_rss_bytes: peak.max(resident_bytes()),
        },
    )
}

#[cfg(target_os = "linux")]
fn resident_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let value = status
        .lines()
        .find_map(|line| line.strip_prefix("VmRSS:"))?;
    value
        .split_whitespace()
        .next()?
        .parse::<u64>()
        .ok()?
        .checked_mul(1024)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> Option<u64> {
    None
}
