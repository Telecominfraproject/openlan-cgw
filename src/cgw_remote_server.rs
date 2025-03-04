use cgw_common::cgw_app_args::CGWGRPCArgs;

pub mod cgw_remote {
    tonic::include_proto!("cgw.remote");
}

use tonic::{transport::Server, Request, Response, Status};

use cgw_remote::{
    remote_server::{Remote, RemoteServer},
    EnqueueRequest, EnqueueResponse,
};

use tokio::time::Duration;

use crate::cgw_remote_discovery::CGWRemoteConfig;

use crate::cgw_connection_server::CGWConnectionServer;

use tokio_stream::StreamExt;

use std::sync::Arc;

struct CGWRemote {
    cgw_srv: Arc<CGWConnectionServer>,
}

#[tonic::async_trait]
impl Remote for CGWRemote {
    async fn enqueue_nbapi_request_stream(
        &self,
        request: Request<tonic::Streaming<EnqueueRequest>>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let mut rq_stream = request.into_inner();
        while let Some(rq) = rq_stream.next().await {
            let rq = rq?;
            self.cgw_srv
                .enqueue_mbox_relayed_message_to_cgw_server(rq.key, rq.req)
                .await;
        }

        let reply = cgw_remote::EnqueueResponse {
            ret: 0,
            msg: "DONE".to_string(),
        };
        Ok(Response::new(reply))
    }
}

pub struct CGWRemoteServer {
    cfg: CGWRemoteConfig,
}

impl CGWRemoteServer {
    pub fn new(cgw_id: i32, grpc_args: &CGWGRPCArgs) -> Self {
        let remote_cfg = CGWRemoteConfig::new(
            cgw_id,
            grpc_args.grpc_listening_ip,
            grpc_args.grpc_listening_port,
        );
        CGWRemoteServer { cfg: remote_cfg }
    }
    pub async fn start(&self, srv: Arc<CGWConnectionServer>) {
        // GRPC server
        // TODO: use CGWRemoteServerConfig wrap
        let cgw_server = CGWRemote { cgw_srv: srv };
        let grpc_srv = Server::builder()
            .tcp_keepalive(Some(Duration::from_secs(7200)))
            .http2_keepalive_timeout(Some(Duration::from_secs(7200)))
            .http2_keepalive_interval(Some(Duration::from_secs(7200)))
            .http2_max_pending_accept_reset_streams(Some(1000))
            .add_service(RemoteServer::new(cgw_server));

        info!(
            "Starting GRPC server id {} - listening at {}:{}",
            self.cfg.remote_id, self.cfg.server_ip, self.cfg.server_port
        );

        if let Err(e) = grpc_srv.serve(self.cfg.to_socket_addr()).await {
            error!("gRPC server failed! Error: {e}");
        };

        // end of gRPC server build / start declaration
    }
}
