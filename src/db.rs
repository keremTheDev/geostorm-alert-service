use sqlx::PgPool;

use crate::models::SpaceWeatherAlert;

pub async fn insert_alert_log(
    pool: &PgPool,
    alert: &SpaceWeatherAlert,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        INSERT INTO alert_logs (activity_id, alert_level, details, event_timestamp)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(&alert.activity_id)
    .bind(&alert.alert_level)
    .bind(&alert.details)
    .bind(alert.timestamp)
    .execute(pool)
    .await?;

    Ok(())
}
