use crate::error::ProxyError;
use pg_wire::{startup::encode_startup, SSL_REQUEST};
use rustls::pki_types::CertificateDer;
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

#[derive(Debug, Clone)]
pub struct UpstreamConfig {
    pub addr: String,
    pub tls: bool,
    pub ca_pem: Option<Vec<u8>>,
    pub tls_verify: bool,
}

type TlsStream = tokio_rustls::client::TlsStream<TcpStream>;

pub enum UpstreamConn {
    Plain(TcpStream),
    Tls(Box<TlsStream>),
}

impl UpstreamConn {
    pub fn peer_plain(&self) -> Option<&TcpStream> {
        match self {
            Self::Plain(s) => Some(s),
            Self::Tls(s) => Some(s.get_ref().0),
        }
    }

    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.read_exact(buf).await,
            Self::Tls(s) => s.read_exact(buf).await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }

    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.write_all(buf).await,
            Self::Tls(s) => s.write_all(buf).await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.shutdown().await,
            Self::Tls(s) => s.shutdown().await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }
}

pub async fn connect_plain(addr: &str) -> Result<TcpStream, ProxyError> {
    TcpStream::connect(addr)
        .await
        .map_err(|e| ProxyError::Connect(e.to_string()))
}

fn build_tls_connector(cfg: &UpstreamConfig) -> Result<TlsConnector, ProxyError> {
    let mut root_store = rustls::RootCertStore::empty();
    if let Some(pem) = &cfg.ca_pem {
        let mut cursor = Cursor::new(pem);
        let certs = rustls_pemfile::certs(&mut cursor)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ProxyError::Tls(e.to_string()))?;
        for cert in certs {
            let _ = root_store.add(cert);
        }
    } else if cfg.tls_verify {
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }
    let builder = rustls::ClientConfig::builder();
    let config = if cfg.tls_verify {
        builder
            .with_root_certificates(root_store)
            .with_no_client_auth()
    } else {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifier))
            .with_no_client_auth()
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

#[derive(Debug)]
struct NoVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::RSA_PKCS1_SHA256,
            rustls::SignatureScheme::RSA_PKCS1_SHA384,
            rustls::SignatureScheme::RSA_PKCS1_SHA512,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::ECDSA_NISTP384_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA384,
            rustls::SignatureScheme::RSA_PSS_SHA512,
            rustls::SignatureScheme::ED25519,
        ]
    }
}

pub async fn connect_and_startup(
    cfg: &UpstreamConfig,
    version: i32,
    params: &[u8],
) -> Result<UpstreamConn, ProxyError> {
    if cfg.tls {
        let mut stream = connect_plain(&cfg.addr).await?;
        stream.write_all(&(8_i32).to_be_bytes()).await?;
        stream.write_all(&SSL_REQUEST.to_be_bytes()).await?;
        let mut reply = [0u8; 1];
        stream.read_exact(&mut reply).await?;
        if reply[0] != b'S' {
            return Err(ProxyError::Tls("upstream did not accept SSLRequest".into()));
        }
        let connector = build_tls_connector(cfg)?;
        let host = extract_host(&cfg.addr);
        let server_name = rustls::pki_types::ServerName::try_from(host)
            .map_err(|_| ProxyError::Tls("invalid server name".into()))?;
        let mut tls = connector
            .connect(server_name, stream)
            .await
            .map_err(|e| ProxyError::Tls(e.to_string()))?;
        tls.write_all(&encode_startup(version, params)).await?;
        Ok(UpstreamConn::Tls(Box::new(tls)))
    } else {
        let mut stream = connect_plain(&cfg.addr).await?;
        stream.write_all(&encode_startup(version, params)).await?;
        Ok(UpstreamConn::Plain(stream))
    }
}

fn extract_host(addr: &str) -> String {
    addr.split(':')
        .next()
        .filter(|h| !h.is_empty())
        .unwrap_or("localhost")
        .to_string()
}

pub enum UpstreamRead {
    Plain(ReadHalf<TcpStream>),
    Tls(Box<ReadHalf<TlsStream>>),
}

pub enum UpstreamWrite {
    Plain(WriteHalf<TcpStream>),
    Tls(Box<WriteHalf<TlsStream>>),
}

impl UpstreamRead {
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.read_exact(buf).await,
            Self::Tls(s) => s.read_exact(buf).await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }

    pub async fn read_frame_header(&mut self) -> Result<(u8, usize), ProxyError> {
        match self {
            Self::Plain(s) => pg_wire::frame::read_frame_header(s).await,
            Self::Tls(s) => pg_wire::frame::read_frame_header(s).await,
        }
        .map_err(ProxyError::from)
    }

    pub async fn read_small_frame(
        &mut self,
        tag: u8,
        body_len: usize,
    ) -> Result<pg_wire::frame::Frame, ProxyError> {
        match self {
            Self::Plain(s) => pg_wire::frame::read_small_frame(s, tag, body_len).await,
            Self::Tls(s) => pg_wire::frame::read_small_frame(s, tag, body_len).await,
        }
        .map_err(ProxyError::from)
    }

    pub async fn stream_body_to<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        w: &mut W,
        tag: u8,
        body_len: usize,
    ) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => pg_wire::frame::stream_frame_body(s, w, tag, body_len).await,
            Self::Tls(s) => pg_wire::frame::stream_frame_body(s, w, tag, body_len).await,
        }
        .map_err(ProxyError::from)
    }
}

impl UpstreamWrite {
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.write_all(buf).await,
            Self::Tls(s) => s.write_all(buf).await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }

    pub async fn shutdown(&mut self) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => s.shutdown().await,
            Self::Tls(s) => s.shutdown().await,
        }
        .map_err(ProxyError::from)?;
        Ok(())
    }

    pub async fn write_frame(&mut self, tag: u8, body: &[u8]) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => pg_wire::frame::forward_buffered(s, tag, body).await,
            Self::Tls(s) => pg_wire::frame::forward_buffered(s, tag, body).await,
        }
        .map_err(ProxyError::from)
    }

    pub async fn stream_body_from<R: tokio::io::AsyncRead + Unpin>(
        &mut self,
        r: &mut R,
        tag: u8,
        body_len: usize,
    ) -> Result<(), ProxyError> {
        match self {
            Self::Plain(s) => pg_wire::frame::stream_frame_body(r, s, tag, body_len).await,
            Self::Tls(s) => pg_wire::frame::stream_frame_body(r, s, tag, body_len).await,
        }
        .map_err(ProxyError::from)
    }
}

pub fn split_upstream(conn: UpstreamConn) -> (UpstreamRead, UpstreamWrite) {
    match conn {
        UpstreamConn::Plain(s) => {
            let (r, w) = split(s);
            (UpstreamRead::Plain(r), UpstreamWrite::Plain(w))
        }
        UpstreamConn::Tls(s) => {
            let (r, w) = split(*s);
            (
                UpstreamRead::Tls(Box::new(r)),
                UpstreamWrite::Tls(Box::new(w)),
            )
        }
    }
}
