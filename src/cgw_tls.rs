use crate::cgw_errors::{collect_results, Error, Result};
use crate::AppArgs;

use eui48::MacAddress;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::{fs::File, io::BufReader, str::FromStr, sync::Arc};
use tokio::net::TcpStream;
use tokio_rustls::{
    rustls::{server::WebPkiClientVerifier, RootCertStore, ServerConfig},
    server::TlsStream,
    TlsAcceptor,
};
use x509_parser::parse_x509_certificate;

const CGW_TLS_CERTIFICATES_PATH: &str = "/etc/cgw/certs";

pub async fn cgw_tls_read_certs(cert_file: &str) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(cert_file)?;
    let mut reader = BufReader::new(file);

    collect_results(rustls_pemfile::certs(&mut reader))
}

pub async fn cgw_tls_read_private_key(private_key_file: &str) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(private_key_file)?;
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)?.ok_or(Error::Tls("Failed to read private key!"))
}

pub async fn cgw_tls_get_cn_from_stream(stream: &TlsStream<TcpStream>) -> Result<MacAddress> {
    let certs = stream
        .get_ref()
        .1
        .peer_certificates()
        .ok_or(Error::Tls("Failed to read peer certificate!"))?;

    let first_cert = certs
        .first()
        .ok_or(Error::Tls("Failed to get first certificate!"))?;

    match parse_x509_certificate(first_cert.as_ref()) {
        Ok(parsed_cert) => {
            for rdn in parsed_cert.1.subject().iter_common_name() {
                if let Ok(cn) = rdn.as_str() {
                    return Ok(MacAddress::from_str(cn)?);
                }
            }
        }
        Err(_) => {
            return Err(Error::Tls("Failed to read peer comman name!"));
        }
    }

    Err(Error::Tls("Failed to read peer comman name!"))
}
pub async fn cgw_tls_create_acceptor(args: &AppArgs) -> Result<TlsAcceptor> {
    // Read root/issuer certs.
    let cas_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_cas);
    let cas = cgw_tls_read_certs(cas_path.as_str()).await?;

    // Read cert.
    let cert_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_cert);
    let mut cert = cgw_tls_read_certs(cert_path.as_str()).await?;
    cert.extend(cas.clone());

    // Read private key.
    let key_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_key);
    let key = cgw_tls_read_private_key(key_path.as_str()).await?;

    // Create the client certs verifier.
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(cas);

    let client_verifier = WebPkiClientVerifier::builder(Arc::new(roots)).build()?;

    // Create server config.
    let config = ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert, key)?;

    // Create the TLS acceptor.
    Ok(TlsAcceptor::from(Arc::new(config)))
}
