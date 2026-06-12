use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SpaceWeatherAlert {
    #[serde(default = "default_schema_version")]
    pub schema_version: String,
    #[serde(default)]
    pub event_id: String,
    pub activity_id: String,
    pub alert_level: String,
    pub details: String,
    pub timestamp: DateTime<Utc>,
}

impl SpaceWeatherAlert {
    pub fn normalized_event_id(&self) -> String {
        if self.event_id.trim().is_empty() {
            format!("legacy:{}:{}", self.activity_id, self.timestamp.to_rfc3339())
        } else {
            self.event_id.clone()
        }
    }

    pub fn normalized_schema_version(&self) -> String {
        if self.schema_version.trim().is_empty() {
            "legacy".to_string()
        } else {
            self.schema_version.clone()
        }
    }
}

fn default_schema_version() -> String {
    "legacy".to_string()
}
