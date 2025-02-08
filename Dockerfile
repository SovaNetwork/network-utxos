# Build stage
FROM rust:1.81-slim-bookworm as builder

# Create a new empty shell project
WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    pkg-config \
    libssl-dev \
    libsqlite3-dev && \
    rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src
COPY justfile ./

# Build the application
RUN cargo build --release

# Final stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y \
    ca-certificates \
    libsqlite3-0 && \
    rm -rf /var/lib/apt/lists/*

# Copy the binary from builder
COPY --from=builder /usr/src/app/target/release/network-utxos /usr/local/bin/

# Create a non-root user with specific UID and create data directory
RUN useradd -m -u 1001 webhook && \
    mkdir -p /data && \
    chown -R webhook:webhook /data

# Set environment variables
ENV RUST_LOG=info

# Switch to non-root user
USER webhook
WORKDIR /data

# Expose the webhook server port
EXPOSE 5557

# Run the binary
CMD ["network-utxos"]