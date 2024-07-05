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
    let file = match File::open(cert_file) {
        Ok(f) => f,
        Err(e) => {
            return Err(Error::Tls(format!(
                "Failed to open TLS certificate file: {}. Error: {}",
                cert_file, e
            )));
        }
    };

    let mut reader = BufReader::new(file);

    collect_results(rustls_pemfile::certs(&mut reader))
}

pub async fn cgw_tls_read_private_key(private_key_file: &str) -> Result<PrivateKeyDer<'static>> {
    let file = match File::open(private_key_file) {
        Ok(f) => f,
        Err(e) => {
            return Err(Error::Tls(format!(
                "Failed to open TLS private key file: {}. Error: {}",
                private_key_file, e
            )));
        }
    };

    let mut reader = BufReader::new(file);

    match rustls_pemfile::private_key(&mut reader) {
        Ok(ret_pk) => match ret_pk {
            Some(pk) => Ok(pk),
            None => Err(Error::Tls(format!(
                "Private key not found in file: {}",
                private_key_file
            ))),
        },
        Err(e) => Err(Error::Tls(format!(
            "Failed to read private key from file: {}. Error: {}",
            private_key_file, e
        ))),
    }
}

pub async fn cgw_tls_get_cn_from_stream(stream: &TlsStream<TcpStream>) -> Result<MacAddress> {
    let certs = match stream.get_ref().1.peer_certificates() {
        Some(c) => c,
        None => {
            return Err(Error::Tls(
                "Certificates not found in clinet connection!".to_string(),
            ));
        }
    };

    let first_cert = match certs.first() {
        Some(cert) => cert,
        None => {
            return Err(Error::Tls(
                "First certificate not found in clinet connection!".to_string(),
            ));
        }
    };

    match parse_x509_certificate(first_cert.as_ref()) {
        Ok(parsed_cert) => {
            for rdn in parsed_cert.1.subject().iter_common_name() {
                if let Ok(cn) = rdn.as_str() {
                    match MacAddress::from_str(cn) {
                        Ok(mac) => return Ok(mac),
                        Err(e) => {
                            return Err(Error::Tls(format!(
                                "Failed to parse clien CN/MAC. Error: {}",
                                e
                            )))
                        }
                    };
                }
            }
        }
        Err(e) => {
            return Err(Error::Tls(format!(
                "Failed to read peer comman name. Error: {}",
                e
            )));
        }
    }

    Err(Error::Tls("Failed to read peer comman name!".to_string()))
}
pub async fn cgw_tls_create_acceptor(args: &AppArgs) -> Result<TlsAcceptor> {
    // Read root/issuer certs.
    let cas_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_cas);
    let cas = match cgw_tls_read_certs(cas_path.as_str()).await {
        Ok(cas_pem) => cas_pem,
        Err(e) => {
            error!("{}", e.to_string());
            return Err(e);
        }
    };

    // Read cert.
    let cert_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_cert);
    let mut cert = match cgw_tls_read_certs(cert_path.as_str()).await {
        Ok(cert_pem) => cert_pem,
        Err(e) => {
            error!("{}", e.to_string());
            return Err(e);
        }
    };
    cert.extend(cas.clone());

    // Read private key.
    let key_path = format!("{}/{}", CGW_TLS_CERTIFICATES_PATH, args.wss_key);
    let key = match cgw_tls_read_private_key(key_path.as_str()).await {
        Ok(pkey) => pkey,
        Err(e) => {
            error!("{}", e.to_string());
            return Err(e);
        }
    };

    // Create the client certs verifier.
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(cas);

    let client_verifier = match WebPkiClientVerifier::builder(Arc::new(roots)).build() {
        Ok(verifier) => verifier,
        Err(e) => {
            error!("Failed to build client verifier: {}", e.to_string());
            return Err(Error::Tls("Failed to build client verifier!".to_string()));
        }
    };

    // Create server config.
    let config = match ServerConfig::builder()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(cert, key)
    {
        Ok(server_config) => server_config,
        Err(e) => {
            error!("Failed to build server config: {}", e.to_string());
            return Err(Error::Tls("Failed to build server config!".to_string()));
        }
    };

    // Create the TLS acceptor.
    Ok(TlsAcceptor::from(Arc::new(config)))
}
