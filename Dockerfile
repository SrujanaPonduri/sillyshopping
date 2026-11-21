# Shopping Sessions: native Rust pipeline.
# Stage 1 compiles the release binary for the image platform (linux/amd64 for the scorer).
# Stage 2 is a slim runtime with only the binary; the program does no installs, network, or temp files.
FROM rust:1-slim-bookworm AS build
WORKDIR /src
COPY rust/Cargo.toml rust/Cargo.lock ./
COPY rust/src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/shopping-sessions /app/shopping-sessions
ENV PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
USER 1000:1000
CMD ["/app/shopping-sessions"]
