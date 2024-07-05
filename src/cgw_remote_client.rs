pub mod cgw_remote {
    tonic::include_proto!("cgw.remote");
}

use tonic::transport::{channel::Channel, Uri};

use crate::cgw_errors::Result;
use cgw_remote::{remote_client::RemoteClient, EnqueueRequest};

use tokio::time::Duration;

#[derive(Clone)]
pub struct CGWRemoteClient {
    remote_client: RemoteClient<Channel>,
}

impl CGWRemoteClient {
    pub fn new(hostname: String) -> Result<Self> {
        let uri = Uri::from_maybe_shared(hostname)?;
        let r_channel = Channel::builder(uri)
            .timeout(Duration::from_secs(20))
            .connect_timeout(Duration::from_secs(20))
            .connect_lazy();

        let remote_client = RemoteClient::new(r_channel);

        Ok(CGWRemoteClient { remote_client })
    }

    pub async fn relay_request_stream(&self, stream: Vec<(String, String)>) -> Result<()> {
        let mut cl_clone = self.remote_client.clone();
        let mut messages: Vec<EnqueueRequest> = vec![];

        for x in stream.into_iter() {
            messages.push(EnqueueRequest { key: x.0, req: x.1 });
        }

        let rq = tonic::Request::new(tokio_stream::iter(messages.clone()));
        cl_clone.enqueue_nbapi_request_stream(rq).await?;

        Ok(())
    }
}
