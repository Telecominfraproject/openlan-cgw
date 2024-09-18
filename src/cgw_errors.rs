use derive_more::From;

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

    UCentralMessagesQueue(&'static str),

    AppArgsParser(String),

    Runtime(String),

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
    Tokiofs(tokio::fs::ReadDir),

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

    #[from]
    Empty(()),
}

impl ToString for Error {
    fn to_string(&self) -> String {
        match self {
            Error::AppArgsParser(message) => message.clone(),
            Error::Tls(message) => message.clone(),
            _ => format!("{:?}", self),
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
