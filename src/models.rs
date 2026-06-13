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
    #[serde(default)]
    pub current_risk_level: Option<String>,
    #[serde(default)]
    pub forecast_risk_level: Option<String>,
    #[serde(default)]
    pub risk_basis: Option<String>,
    #[serde(default)]
    pub esa_source_status: Option<String>,
    #[serde(default)]
    pub esa_dataset_id: Option<String>,
    #[serde(default)]
    pub esa_error: Option<String>,
    pub details: String,
    pub timestamp: DateTime<Utc>,
}

impl SpaceWeatherAlert {
    pub fn current_risk_level(&self) -> Option<&str> {
        non_empty_optional(self.current_risk_level.as_deref())
    }

    pub fn forecast_risk_level(&self) -> Option<&str> {
        non_empty_optional(self.forecast_risk_level.as_deref())
    }

    pub fn risk_basis(&self) -> Option<&str> {
        non_empty_optional(self.risk_basis.as_deref())
    }

    pub fn esa_source_status(&self) -> Option<&str> {
        non_empty_optional(self.esa_source_status.as_deref())
    }

    pub fn esa_dataset_id(&self) -> Option<&str> {
        non_empty_optional(self.esa_dataset_id.as_deref())
    }

    pub fn esa_error(&self) -> Option<&str> {
        non_empty_optional(self.esa_error.as_deref())
    }
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

fn non_empty_optional(value: Option<&str>) -> Option<&str> {
    value.and_then(|inner| {
        let trimmed = inner.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    })
}
