use crate::AppArgs;

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
            let rq = rq.unwrap();
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
    pub fn new(app_args: &AppArgs) -> Self {
        let remote_cfg =
            CGWRemoteConfig::new(app_args.cgw_id, app_args.grpc_ip, app_args.grpc_port);
        let remote_server = CGWRemoteServer { cfg: remote_cfg };
        remote_server
    }
    pub async fn start(&self, srv: Arc<CGWConnectionServer>) {
        // GRPC server
        // TODO: use CGWRemoteServerConfig wrap
        let icgw_serv = CGWRemote { cgw_srv: srv };
        let grpc_srv = Server::builder()
            .tcp_keepalive(Some(Duration::from_secs(7200)))
            .http2_keepalive_timeout(Some(Duration::from_secs(7200)))
            .http2_keepalive_interval(Some(Duration::from_secs(7200)))
            .http2_max_pending_accept_reset_streams(Some(1000))
            .add_service(RemoteServer::new(icgw_serv));

        info!(
            "Starting GRPC server id {} - listening at {}:{}",
            self.cfg.remote_id, self.cfg.server_ip, self.cfg.server_port
        );
        let res = grpc_srv.serve(self.cfg.to_socket_addr()).await;
        error!("grpc server returned {:?}", res);
        // end of GRPC server build / start declaration
    }
}
