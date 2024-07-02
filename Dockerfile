# Stage 1: Create a rust build image
FROM rust:1.77.0 AS builder
LABEL Description="OpenLan CGW (Build) environment"

# Setup a specific compilation target to the Rust toolchain.
RUN rustup target add x86_64-unknown-linux-gnu

# Install build packages
RUN apt-get update -q -y  && \
    apt-get install -q -y    \
    pkg-config               \
    build-essential          \
    cmake                    \
    protobuf-compiler        \
    libssl-dev

# Create directory - ignore errors if exist
RUN mkdir -p /usr/src/openlan-cgw

# Set the working directory
WORKDIR /usr/src/openlan-cgw

# Mount the Cargo.toml and Cargo.lock files
# Mount the build.rs and src directory
# Mount cargo registry and git db cache and target cache
# Build CGW application
# Copy binary to default location before cache unmounted
RUN --mount=type=bind,source=src,target=src                                \
    --mount=type=bind,source=build.rs,target=build.rs                      \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml                  \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock                  \
    --mount=type=cache,target=/usr/src/openlan-cgw/target                  \
    --mount=type=cache,target=/usr/local/cargo/git/db                      \
    --mount=type=cache,target=/usr/local/cargo/registry                    \
    cargo build --target x86_64-unknown-linux-gnu --release &&             \
    cp target/x86_64-unknown-linux-gnu/release/ucentral-cgw /usr/local/bin

CMD ["echo", "CGW build finished successfully!"]

# Stage 2: Create a runtime image
FROM rust:1.77.0 AS cgw-img

#ARG CGW_CONTAINER_BUILD_REV="<unknown>"
ARG GITHUB_SHA="<unknown>"
ARG GITHUB_BRANCH="<unknown>"
ARG GITHUB_TIME="0"
ENV CGW_CONTAINER_BUILD_REV=$GITHUB_SHA
ENV CGW_CONTAINER_BUILD_BRANCH=$GITHUB_BRANCH
ENV CGW_CONTAINER_BUILD_TIME=$GITHUB_TIME

LABEL Description="OpenLan CGW environment"

# # Create a non-root user and group
RUN adduser cgw_runner && addgroup cgw_users_group

# Add `cgw_runner` user to `cgw_users_group`
RUN usermod -a -G cgw_users_group cgw_runner

# CGW create log file under /var directory
# It is required to change direcory owner
RUN chown cgw_runner:cgw_users_group "/var"

# Switch to non-root user
USER cgw_runner

# Create volume to certificates directory
VOLUME [ "/etc/cgw/certs" ]

# Copy libs and CGW binaries
COPY --from=builder /lib/x86_64-linux-gnu/ /lib/x86_64-linux-gnu/
COPY --from=builder /usr/local/bin/ucentral-cgw /usr/local/bin/ucentral-cgw

CMD ["ucentral-cgw"]
