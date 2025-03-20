use derive_more::From;
use eui48::MacAddressFormat;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, From)]
pub enum Error {
    // -- Internals
    ConnectionProcessor(&'static str),

    ConnectionServer(String),

    DbAccessor(&'static str),

    RemoteDiscovery(&'static str),

    RemoteDiscoveryFailedInfras(Vec<eui48::MacAddress>),

    Tcp(String),

    Tls(String),

    Redis(String),

    UCentralParser(&'static str),

    UCentralValidator(String),

    UCentralMessagesQueue(String),

    AppArgsParser(String),

    Runtime(String),

    KafkaInit(String),

    // -- Externals
    #[from]
    Io(std::io::Error),

    #[from]
    ClientVerifierBuilder(tokio_rustls::rustls::client::VerifierBuilderError),

    #[from]
    TokioPostgres(tokio_postgres::Error),

    #[from]
    TokioRustls(tokio_rustls::rustls::Error),

    #[from]
    TokioSync(tokio::sync::TryLockError),

    #[from]
    IpAddressParse(std::net::AddrParseError),

    #[from]
    MacAddressParse(eui48::ParseError),

    #[from]
    ParseInt(std::num::ParseIntError),

    #[from]
    TryFromInt(std::num::TryFromIntError),

    #[from]
    Prometheus(prometheus::Error),

    #[from]
    SerdeJson(serde_json::Error),

    #[from]
    Kafka(rdkafka::error::KafkaError),

    #[from]
    InvalidUri(warp::http::uri::InvalidUri),

    #[from]
    StaticStr(&'static str),

    #[from]
    Tonic(tonic::Status),

    #[from]
    Tungstenite(tungstenite::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::AppArgsParser(message)
            | Error::Tls(message)
            | Error::ConnectionServer(message)
            | Error::Runtime(message)
            | Error::KafkaInit(message)
            | Error::Redis(message)
            | Error::Tcp(message)
            | Error::UCentralMessagesQueue(message)
            | Error::UCentralValidator(message) => write!(f, "{}", message),
            Error::ConnectionProcessor(message)
            | Error::DbAccessor(message)
            | Error::RemoteDiscovery(message)
            | Error::UCentralParser(message)
            | Error::StaticStr(message) => write!(f, "{}", message),
            Error::Io(io_error) => write!(f, "{}", io_error),
            Error::ClientVerifierBuilder(verifier_error) => write!(f, "{}", verifier_error),
            Error::TokioPostgres(psql_error) => write!(f, "{}", psql_error),
            Error::TokioRustls(rustls_error) => write!(f, "{}", rustls_error),
            Error::TokioSync(sync_error) => write!(f, "{}", sync_error),
            Error::IpAddressParse(ip_parse_error) => write!(f, "{}", ip_parse_error),
            Error::MacAddressParse(mac_parse_error) => write!(f, "{}", mac_parse_error),
            Error::ParseInt(int_error) => write!(f, "{}", int_error),
            Error::TryFromInt(try_from_int_error) => write!(f, "{}", try_from_int_error),
            Error::Prometheus(prometheus_error) => write!(f, "{}", prometheus_error),
            Error::SerdeJson(serde_error) => write!(f, "{}", serde_error),
            Error::Kafka(kafka_error) => write!(f, "{}", kafka_error),
            Error::InvalidUri(uri_error) => write!(f, "{}", uri_error),
            Error::Tonic(tonic_error) => write!(f, "{}", tonic_error),
            Error::Tungstenite(tungstenite_error) => write!(f, "{}", tungstenite_error),
            Error::RemoteDiscoveryFailedInfras(vec) => {
                let result = vec
                    .iter()
                    .map(|obj| obj.to_string(MacAddressFormat::HexString))
                    .collect::<Vec<_>>()
                    .join(", ");

                write!(f, "{}", result)
            }
        }
    }
}

// Helper function to collect results
pub fn collect_results<I, T, E>(iter: I) -> Result<Vec<T>>
where
    I: IntoIterator<Item = std::result::Result<T, E>>,
    Error: From<E>,
{
    let mut vec = Vec::new();

    for item in iter {
        match item {
            Ok(value) => vec.push(value),
            Err(e) => return Err(e.into()),
        }
    }

    Ok(vec)
}
