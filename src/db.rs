use sqlx::PgPool;

use crate::models::SpaceWeatherAlert;

pub async fn insert_alert_log(
    pool: &PgPool,
    alert: &SpaceWeatherAlert,
) -> Result<bool, sqlx::Error> {
    let result = sqlx::query_scalar::<_, i64>(
        r#"
        INSERT INTO alert_logs (
            event_id,
            schema_version,
            activity_id,
            alert_level,
            details,
            event_timestamp
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT DO NOTHING
        RETURNING id
        "#,
    )
    .bind(alert.normalized_event_id())
    .bind(alert.normalized_schema_version())
    .bind(&alert.activity_id)
    .bind(&alert.alert_level)
    .bind(&alert.details)
    .bind(alert.timestamp)
    .fetch_optional(pool)
    .await?;

    Ok(result.is_some())
}

pub async fn update_alert_processing_result(
    pool: &PgPool,
    event_id: &str,
    report_path: Option<&str>,
    email_status: &str,
    error_message: Option<&str>,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        UPDATE alert_logs
        SET report_path = $2,
            email_status = $3,
            processed_at = NOW(),
            error_message = $4
        WHERE event_id = $1
        "#,
    )
    .bind(event_id)
    .bind(report_path)
    .bind(email_status)
    .bind(error_message)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_existing_report_path(
    pool: &PgPool,
    event_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar(
        r#"
        SELECT report_path
        FROM alert_logs
        WHERE event_id = $1
          AND report_path IS NOT NULL
        "#,
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await
}
