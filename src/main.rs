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
use tokio::task::JoinHandle;

use crate::{models::SpaceWeatherAlert, rabbitmq::ConsumerContext};

const REPORTS_DIR: &str = "reports";

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

    println!("[startup] Connecting to RabbitMQ");
    let rabbitmq_connection = rabbitmq::connect(&rabbitmq_url).await?;
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

    println!("[runtime] GeoStorm Alert Service is listening. Press Ctrl+C to shut down.");
    tokio::signal::ctrl_c().await?;

    println!("[shutdown] Shutdown signal received");
    consumer_task.abort();

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
        Ok(report_path) => {
            println!(
                "[consumer] Alert processed successfully activity_id={} report={}",
                alert.activity_id,
                report_path.display()
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
) -> ServiceResult<PathBuf> {
    println!("[db] Inserting alert log activity_id={}", alert.activity_id);
    db::insert_alert_log(db_pool, alert).await?;
    println!("[db] Alert log inserted activity_id={}", alert.activity_id);

    let report_path = report::generate_alert_report(alert, reports_dir).await?;
    Ok(report_path)
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
