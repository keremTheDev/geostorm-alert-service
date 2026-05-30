use lapin::{
    options::{
        BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions, QueueBindOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    Channel, Connection, ConnectionProperties, Consumer, ExchangeKind,
};

pub const ALERT_EXCHANGE: &str = "space_weather";
pub const ALERT_QUEUE: &str = "space_weather_alerts";
pub const ALERT_ROUTING_KEY: &str = "space_weather_alerts";

pub struct ConsumerContext {
    pub channel: Channel,
    pub consumer: Consumer,
}

pub async fn connect(rabbitmq_url: &str) -> Result<Connection, lapin::Error> {
    let properties =
        ConnectionProperties::default().with_executor(tokio_executor_trait::Tokio::current());

    Connection::connect(rabbitmq_url, properties).await
}

pub async fn configure_consumer(connection: &Connection) -> Result<ConsumerContext, lapin::Error> {
    let channel = connection.create_channel().await?;

    channel
        .exchange_declare(
            ALERT_EXCHANGE,
            ExchangeKind::Direct,
            ExchangeDeclareOptions {
                durable: true,
                ..ExchangeDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_declare(
            ALERT_QUEUE,
            QueueDeclareOptions {
                durable: true,
                ..QueueDeclareOptions::default()
            },
            FieldTable::default(),
        )
        .await?;

    channel
        .queue_bind(
            ALERT_QUEUE,
            ALERT_EXCHANGE,
            ALERT_ROUTING_KEY,
            QueueBindOptions::default(),
            FieldTable::default(),
        )
        .await?;

    channel
        .basic_qos(16, BasicQosOptions::default())
        .await?;

    let consumer = channel
        .basic_consume(
            ALERT_QUEUE,
            "geostorm-alert-service",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await?;

    Ok(ConsumerContext { channel, consumer })
}
