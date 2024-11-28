# build rust binary
alias b := build

build:
    cargo build --release

# Run with defaults (localhost)
run:
    cargo run --release

# Run for local development
run-dev:
    RUST_LOG=info cargo run --release -- \
        --host 127.0.0.1 \
        --port 5557 \
        --log-level info \
        --datasource sqlite

# Run for docker/production (bind to all interfaces)
run-docker:
    RUST_LOG=info cargo run --release -- \
        --host 0.0.0.0 \
        --port 5557 \
        --log-level info \
        --datasource sqlite

# Run with custom parameters
run-custom host port log_level datasource:
    RUST_LOG={{log_level}} cargo run --release -- \
        --host {{host}} \
        --port {{port}} \
        --log-level {{log_level}} \
        --datasource {{datasource}}
