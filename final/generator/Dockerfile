# ---------- stage 1: build ----------
# Use bookworm base so GLIBC matches the runtime image (rust:1-slim is trixie => GLIBC 2.38+).
FROM rust:1-bookworm AS build
WORKDIR /src
# pre-fetch deps with a stub main to leverage docker layer caching
COPY Cargo.toml ./
RUN mkdir src && echo 'fn main(){}' > src/main.rs && cargo build --release && rm -rf src target/release/generator target/release/deps/generator*
COPY src ./src
RUN touch src/main.rs && cargo build --release

# ---------- stage 2: runtime ----------
FROM debian:bookworm-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    useradd -m -u 1001 app && \
    mkdir -p /var/cache/generator && chown -R app /var/cache/generator
USER app
COPY --from=build /src/target/release/generator /usr/local/bin/generator
ENTRYPOINT ["/usr/local/bin/generator"]
