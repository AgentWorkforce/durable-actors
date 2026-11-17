use super::*;
use crate::bucket::ArchiveBatchConfig;

const MAX: Duration = Duration::from_secs(5);

#[test]
fn lag_alarm_fires_once_per_episode_and_reports_the_peak() {
    let mut alarm = LagAlarm::new(MAX);
    assert_eq!(alarm.transition(None), None);
    assert_eq!(alarm.transition(Some(Duration::from_secs(4))), None);
    assert_eq!(
        alarm.transition(Some(Duration::from_secs(6))),
        Some(LagChange::Exceeded(Duration::from_secs(6)))
    );
    assert_eq!(alarm.transition(Some(Duration::from_secs(9))), None);
    assert_eq!(alarm.transition(Some(Duration::from_secs(7))), None);
    assert_eq!(
        alarm.transition(None),
        Some(LagChange::Recovered(Duration::from_secs(9)))
    );
    assert_eq!(alarm.transition(Some(Duration::from_secs(1))), None);
    assert_eq!(
        alarm.transition(Some(Duration::from_secs(6))),
        Some(LagChange::Exceeded(Duration::from_secs(6)))
    );
}

#[test]
fn max_lag_defaults_to_three_intervals_and_must_exceed_one() {
    let mut config = ArchiveBatchConfig {
        bytes: 1024,
        interval_ms: 2_000,
        max_lag_ms: None,
    };
    assert_eq!(config.max_lag(), Duration::from_secs(6));
    config.max_lag_ms = Some(5_000);
    assert_eq!(config.max_lag(), Duration::from_secs(5));
    assert!(config.validate().is_ok());
    config.max_lag_ms = Some(2_000);
    assert!(config.validate().is_err());
}

#[test]
fn archive_config_without_max_lag_keeps_its_wire_format() -> Result<()> {
    let config = ArchiveBatchConfig::default();
    let encoded = serde_json::to_value(config)?;
    assert!(encoded.get("max_lag_ms").is_none());
    let decoded: ArchiveBatchConfig =
        serde_json::from_value(serde_json::json!({"bytes": 1, "interval_ms": 1}))?;
    assert_eq!(decoded.max_lag_ms, None);
    Ok(())
}
