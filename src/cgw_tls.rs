use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::{
    fs::File,
    io::{BufReader, Error, ErrorKind},
};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use x509_parser::parse_x509_certificate;

pub async fn cgw_tls_read_certs(cert_file: &str) -> Result<Vec<CertificateDer<'static>>, Error> {
    let file = match File::open(cert_file) {
        Ok(file) => file,
        Err(err) => return Err(err),
    };

    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader).collect::<Result<Vec<_>, _>>()
}

pub async fn cgw_tls_read_private_key(
    private_key_file: &str,
) -> Result<PrivateKeyDer<'static>, Error> {
    let file = match File::open(private_key_file) {
        Ok(file) => file,
        Err(err) => return Err(err),
    };

    let mut reader = BufReader::new(file);
    match rustls_pemfile::private_key(&mut reader) {
        Ok(key) => return key.ok_or_else(|| Error::new(ErrorKind::Other, "Option is None")),
        Err(err) => return Err(err),
    };
}

pub async fn cgw_tls_get_cn_from_stream(stream: &TlsStream<TcpStream>) -> Option<String> {
    if let Some(certs) = stream.get_ref().1.peer_certificates() {
        let first_cert = certs.get(0).unwrap();

        match parse_x509_certificate(first_cert.as_ref()) {
            Ok(parsed_cert) => {
                for rdn in parsed_cert.1.subject().iter_common_name() {
                    if let Ok(cn) = rdn.as_str() {
                        return Some(cn.to_owned());
                    }
                }
            }
            Err(_) => {
                return None;
            }
        }
    }

    None
}
