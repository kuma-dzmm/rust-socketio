use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use crate::asynchronous::transport::AsyncTransport;
use crate::error::Result;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::StreamExt;
use futures_util::Stream;
use http::HeaderMap;
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tungstenite::client::IntoClientRequest;
use url::Url;

use super::websocket_general::AsyncWebsocketGeneralTransport;

/// An asynchronous websocket transport type.
/// This type only allows for plain websocket
/// connections ("ws://").
#[derive(Clone)]
pub struct WebsocketTransport {
    inner: AsyncWebsocketGeneralTransport,
    base_url: Arc<RwLock<Url>>,
}

impl WebsocketTransport {
    /// Creates a new instance over a request that might hold additional headers and an URL.
    pub async fn new(base_url: Url, headers: Option<HeaderMap>) -> Result<Self> {
        let url = Self::websocket_url(base_url);
        let mut req = url.as_str().into_client_request()?;
        if let Some(map) = headers {
            // SAFETY: this unwrap never panics as the underlying request is just initialized and in proper state
            req.headers_mut().extend(map);
        }

        let (ws_stream, _) = connect_async(req).await?;
        Self::from_prepared_websocket_stream(url, ws_stream).await
    }

    pub(crate) async fn from_websocket_stream(
        base_url: Url,
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<Self> {
        let url = Self::websocket_url(base_url);
        Self::from_prepared_websocket_stream(url, stream).await
    }

    async fn from_prepared_websocket_stream(
        url: Url,
        stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> Result<Self> {
        let inner = AsyncWebsocketGeneralTransport::from_stream(stream).await;
        Ok(WebsocketTransport {
            inner,
            base_url: Arc::new(RwLock::new(url)),
        })
    }

    fn websocket_url(mut base_url: Url) -> Url {
        base_url.query_pairs_mut().append_pair("transport", "websocket");
        base_url.set_scheme("ws").unwrap();
        base_url
    }

    /// Sends probe packet to ensure connection is valid, then sends upgrade
    /// request
    pub(crate) async fn upgrade(&self) -> Result<()> {
        self.inner.upgrade().await
    }

    pub(crate) async fn poll_next(&self) -> Result<Option<Bytes>> {
        self.inner.poll_next().await
    }
}

#[async_trait]
impl AsyncTransport for WebsocketTransport {
    async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        self.inner.emit(data, is_binary_att).await
    }

    async fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read().await.clone())
    }

    async fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "websocket")
        {
            url.query_pairs_mut().append_pair("transport", "websocket");
        }
        url.set_scheme("ws").unwrap();
        *self.base_url.write().await = url;
        Ok(())
    }
}

impl Stream for WebsocketTransport {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl Debug for WebsocketTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AsyncWebsocketTransport")
            .field(
                "base_url",
                &self
                    .base_url
                    .try_read()
                    .map_or("Currently not available".to_owned(), |url| url.to_string()),
            )
            .finish()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::ENGINE_IO_VERSION;
    use std::str::FromStr;

    async fn new() -> Result<WebsocketTransport> {
        let url = crate::test::engine_io_server()?.to_string()
            + "engine.io/?EIO="
            + &ENGINE_IO_VERSION.to_string();
        WebsocketTransport::new(Url::from_str(&url[..])?, None).await
    }

    #[tokio::test]
    async fn websocket_transport_base_url() -> Result<()> {
        let transport = new().await?;
        let mut url = crate::test::engine_io_server()?;
        url.set_path("/engine.io/");
        url.query_pairs_mut()
            .append_pair("EIO", &ENGINE_IO_VERSION.to_string())
            .append_pair("transport", "websocket");
        url.set_scheme("ws").unwrap();
        assert_eq!(transport.base_url().await?.to_string(), url.to_string());
        transport
            .set_base_url(reqwest::Url::parse("https://127.0.0.1")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "ws://127.0.0.1/?transport=websocket"
        );
        assert_ne!(transport.base_url().await?.to_string(), url.to_string());

        transport
            .set_base_url(reqwest::Url::parse(
                "http://127.0.0.1/?transport=websocket",
            )?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "ws://127.0.0.1/?transport=websocket"
        );
        assert_ne!(transport.base_url().await?.to_string(), url.to_string());
        Ok(())
    }

    #[tokio::test]
    async fn websocket_secure_debug() -> Result<()> {
        let mut transport = new().await?;
        assert_eq!(
            format!("{:?}", transport),
            format!(
                "AsyncWebsocketTransport {{ base_url: {:?} }}",
                transport.base_url().await?.to_string()
            )
        );
        println!("{:?}", transport.next().await.unwrap());
        println!("{:?}", transport.next().await.unwrap());
        Ok(())
    }
}
