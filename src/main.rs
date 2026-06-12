mod db;
mod models;
mod rabbitmq;
mod report;

use std::{
    env,
    error::Error,
    future,
    path::{Path, PathBuf},
};

use lapin::{
    message::{Delivery, DeliveryResult},
    options::{BasicAckOptions, BasicNackOptions, BasicRejectOptions},
};
use sqlx::PgPool;
use tokio::{task::JoinHandle, time};

use crate::{models::SpaceWeatherAlert, rabbitmq::ConsumerContext};

const REPORTS_DIR: &str = "reports";
const RABBITMQ_CONNECT_ATTEMPTS: u32 = 12;
const RABBITMQ_CONNECT_RETRY_DELAY_SECONDS: u64 = 5;

type ServiceResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

#[tokio::main]
async fn main() -> ServiceResult<()> {
    dotenvy::dotenv().ok();

    println!("[startup] GeoStorm Alert Service initializing");

    let database_url = env::var("DATABASE_URL")?;
    let rabbitmq_url = env::var("RABBITMQ_URL")?;

    println!("[startup] Connecting to PostgreSQL");
    let db_pool = PgPool::connect(&database_url).await?;
    println!("[startup] PostgreSQL connection pool ready");

    println!("[startup] Running PostgreSQL migrations");
    sqlx::migrate!("./migrations").run(&db_pool).await?;
    println!("[startup] PostgreSQL migrations complete");

    println!("[startup] Connecting to RabbitMQ");
    let rabbitmq_connection = connect_rabbitmq_with_retry(&rabbitmq_url).await?;
    println!("[startup] RabbitMQ connection ready");

    println!(
        "[startup] Declaring exchange={} queue={} routing_key={}",
        rabbitmq::ALERT_EXCHANGE,
        rabbitmq::ALERT_QUEUE,
        rabbitmq::ALERT_ROUTING_KEY
    );
    let consumer_context = rabbitmq::configure_consumer(&rabbitmq_connection).await?;
    println!("[startup] RabbitMQ consumer configured");

    let consumer_task =
        spawn_consumer_task(consumer_context, db_pool.clone(), PathBuf::from(REPORTS_DIR));
    let heartbeat_task = spawn_heartbeat_task();

    println!("[runtime] GeoStorm Alert Service is listening. Press Ctrl+C to shut down.");
    tokio::signal::ctrl_c().await?;

    println!("[shutdown] Shutdown signal received");
    consumer_task.abort();
    heartbeat_task.abort();

    if let Err(error) = rabbitmq_connection
        .close(200, "GeoStorm Alert Service shutting down")
        .await
    {
        eprintln!("[shutdown] RabbitMQ close failed: {error}");
    } else {
        println!("[shutdown] RabbitMQ connection closed");
    }

    db_pool.close().await;
    println!("[shutdown] PostgreSQL pool closed");
    println!("[shutdown] GeoStorm Alert Service stopped");

    Ok(())
}

async fn connect_rabbitmq_with_retry(rabbitmq_url: &str) -> ServiceResult<lapin::Connection> {
    for attempt in 1..=RABBITMQ_CONNECT_ATTEMPTS {
        match rabbitmq::connect(rabbitmq_url).await {
            Ok(connection) => return Ok(connection),
            Err(error) if attempt < RABBITMQ_CONNECT_ATTEMPTS => {
                eprintln!(
                    "[startup] RabbitMQ connection attempt {attempt}/{RABBITMQ_CONNECT_ATTEMPTS} failed: {error}; retrying"
                );
                time::sleep(time::Duration::from_secs(
                    RABBITMQ_CONNECT_RETRY_DELAY_SECONDS,
                ))
                .await;
            }
            Err(error) => return Err(Box::new(error)),
        }
    }

    unreachable!("RabbitMQ retry loop exits via success or final error")
}

fn spawn_consumer_task(
    consumer_context: ConsumerContext,
    db_pool: PgPool,
    reports_dir: PathBuf,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let _channel_guard = consumer_context.channel;
        let consumer = consumer_context.consumer;

        consumer.set_delegate(move |delivery: DeliveryResult| {
            let db_pool = db_pool.clone();
            let reports_dir = reports_dir.clone();

            async move {
                handle_delivery(delivery, db_pool, reports_dir).await;
            }
        });

        println!(
            "[consumer] Delegate registered for queue={}",
            rabbitmq::ALERT_QUEUE
        );

        future::pending::<()>().await;
    })
}

fn spawn_heartbeat_task() -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = time::interval(time::Duration::from_secs(30));

        loop {
            interval.tick().await;
            println!("[health] geostorm-alert-service heartbeat");
        }
    })
}

async fn handle_delivery(delivery_result: DeliveryResult, db_pool: PgPool, reports_dir: PathBuf) {
    match delivery_result {
        Ok(Some(delivery)) => {
            println!(
                "[consumer] Received message delivery_tag={} bytes={}",
                delivery.delivery_tag,
                delivery.data.len()
            );

            handle_alert_delivery(delivery, &db_pool, &reports_dir).await;
        }
        Ok(None) => {
            println!("[consumer] RabbitMQ consumer was cancelled by the broker");
        }
        Err(error) => {
            eprintln!("[consumer] Failed to receive RabbitMQ delivery: {error}");
        }
    }
}

async fn handle_alert_delivery(delivery: Delivery, db_pool: &PgPool, reports_dir: &Path) {
    let alert = match serde_json::from_slice::<SpaceWeatherAlert>(&delivery.data) {
        Ok(alert) => alert,
        Err(error) => {
            eprintln!("[consumer] Invalid alert JSON; rejecting without requeue: {error}");
            reject_without_requeue(&delivery).await;
            return;
        }
    };

    println!(
        "[consumer] Processing alert activity_id={} alert_level={}",
        alert.activity_id, alert.alert_level
    );

    match process_alert(db_pool, reports_dir, &alert).await {
        Ok(Some(report_path)) => {
            println!(
                "[consumer] Alert processed successfully event_id={} activity_id={} report={}",
                alert.normalized_event_id(),
                alert.activity_id,
                report_path.display()
            );
            ack_delivery(&delivery).await;
        }
        Ok(None) => {
            println!(
                "[consumer] Duplicate alert skipped event_id={} activity_id={}",
                alert.normalized_event_id(),
                alert.activity_id
            );
            ack_delivery(&delivery).await;
        }
        Err(error) => {
            eprintln!(
                "[consumer] Alert processing failed activity_id={}: {error}",
                alert.activity_id
            );
            nack_with_requeue(&delivery).await;
        }
    }
}

async fn process_alert(
    db_pool: &PgPool,
    reports_dir: &Path,
    alert: &SpaceWeatherAlert,
) -> ServiceResult<Option<PathBuf>> {
    let event_id = alert.normalized_event_id();
    println!(
        "[db] Inserting alert log event_id={} activity_id={}",
        event_id, alert.activity_id
    );
    let inserted = db::insert_alert_log(db_pool, alert).await?;
    if !inserted {
        if db::get_existing_report_path(db_pool, &event_id).await?.is_some() {
            return Ok(None);
        }

        println!(
            "[consumer] Existing event has no completed report; retrying event_id={}",
            event_id
        );
    }
    println!("[db] Alert log inserted event_id={}", event_id);

    let report_path = match report::generate_alert_report(alert, reports_dir).await {
        Ok(path) => path,
        Err(error) => {
            let error_message = error.to_string();
            let _ = db::update_alert_processing_result(
                db_pool,
                &event_id,
                None,
                "disabled",
                Some(&error_message),
            )
            .await;
            return Err(Box::new(error));
        }
    };
    let report_path_string = report_path.display().to_string();
    let email_status = email_status();

    db::update_alert_processing_result(
        db_pool,
        &event_id,
        Some(&report_path_string),
        &email_status,
        None,
    )
    .await?;

    Ok(Some(report_path))
}

fn email_status() -> String {
    match env::var("EMAIL_ENABLED") {
        Ok(value) if value.eq_ignore_ascii_case("true") => {
            let required = [
                "SMTP_HOST",
                "SMTP_PORT",
                "SMTP_USERNAME",
                "SMTP_PASSWORD",
                "SMTP_FROM",
                "ALERT_EMAIL_TO",
            ];

            if required.iter().all(|key| env::var(key).is_ok_and(|value| !value.is_empty())) {
                "not_implemented".to_string()
            } else {
                "not_configured".to_string()
            }
        }
        _ => "disabled".to_string(),
    }
}

async fn ack_delivery(delivery: &Delivery) {
    match delivery.ack(BasicAckOptions::default()).await {
        Ok(_) => println!(
            "[consumer] Acknowledged message delivery_tag={}",
            delivery.delivery_tag
        ),
        Err(error) => eprintln!(
            "[consumer] Failed to acknowledge delivery_tag={}: {error}",
            delivery.delivery_tag
        ),
    }
}

async fn reject_without_requeue(delivery: &Delivery) {
    match delivery
        .reject(BasicRejectOptions { requeue: false })
        .await
    {
        Ok(_) => println!(
            "[consumer] Rejected invalid message delivery_tag={} requeue=false",
            delivery.delivery_tag
        ),
        Err(error) => eprintln!(
            "[consumer] Failed to reject delivery_tag={}: {error}",
            delivery.delivery_tag
        ),
    }
}

async fn nack_with_requeue(delivery: &Delivery) {
    match delivery
        .nack(BasicNackOptions {
            multiple: false,
            requeue: true,
        })
        .await
    {
        Ok(_) => println!(
            "[consumer] Nacked message delivery_tag={} requeue=true",
            delivery.delivery_tag
        ),
        Err(error) => eprintln!(
            "[consumer] Failed to nack delivery_tag={}: {error}",
            delivery.delivery_tag
        ),
    }
}
