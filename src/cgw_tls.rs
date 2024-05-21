use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::{
    fs::File,
    io::{BufReader, Error, ErrorKind},
};

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
