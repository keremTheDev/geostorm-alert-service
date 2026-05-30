use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SpaceWeatherAlert {
    pub activity_id: String,
    pub alert_level: String,
    pub details: String,
    pub timestamp: DateTime<Utc>,
}
