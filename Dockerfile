FROM rust:1.77.0 as builder
LABEL Description="OpenLan CGW (Build) environment"

WORKDIR /usr/src/openlan-cgw

RUN rustup target add x86_64-unknown-linux-gnu

RUN apt-get update -q -y  && apt-get install -q -y \
	pkg-config \
	build-essential \
	cmake \
	protobuf-compiler \
	libssl-dev

CMD ["make", "-C", "/usr/src/openlan-cgw", "cgw-app"]

FROM rust:1.77.0 as cgw-img
RUN adduser cgw_runner && addgroup cgw_users_group
RUN usermod -a -G cgw_users_group cgw_runner
RUN chown cgw_runner:cgw_users_group "/var"
USER cgw_runner
VOLUME [ "/etc/cgw/certs" ]
COPY --from=builder /lib/x86_64-linux-gnu/ /lib/x86_64-linux-gnu/
COPY output/bin/ucentral-cgw /usr/local/bin/ucentral-cgw
CMD ["ucentral-cgw"]
