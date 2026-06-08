FROM rust:latest AS build

WORKDIR /app

COPY Cargo.toml ./
COPY src ./src
COPY migrations ./migrations

RUN cargo build --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=build /app/target/release/geostorm-alert-service /usr/local/bin/geostorm-alert-service

RUN mkdir -p /app/reports

CMD ["geostorm-alert-service"]
