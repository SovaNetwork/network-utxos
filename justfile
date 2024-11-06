# build rust binary
alias b := build

build:
    cargo build --release

run:
    RUST_LOG=info cargo run --release
