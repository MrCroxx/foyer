//  Copyright 2024 Foyer Project Authors.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::net::SocketAddr;

use foyer_storage::metrics::get_metrics_registry;
use http_body_util::Full;
use hyper::{
    body::{Body, Bytes},
    header::CONTENT_TYPE,
    service::service_fn,
    Request, Response,
};
use prometheus::{Encoder, TextEncoder};
use tokio::net::TcpListener;

pub struct MetricsExporter;

impl MetricsExporter {
    pub fn init(addr: SocketAddr) {
        tokio::spawn(async move {
            tracing::info!("Prometheus service is set up on http://{}", addr);

            let listener = TcpListener::bind(addr).await.unwrap();
            loop {
                let stream = match listener.accept().await {
                    Ok((stream, _addr)) => stream,
                    Err(e) => {
                        tracing::error!("accept conn error: {}", e);
                        continue;
                    }
                };
                let io = hyper_util::rt::TokioIo::new(stream);
                tokio::spawn(async move {
                    if let Err(e) = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new())
                        .serve_connection(io, service_fn(Self::serve))
                        .await
                    {
                        tracing::error!("Prometheus service error: {}", e);
                    }
                });
            }
        });
    }

    async fn serve(_request: Request<impl Body + Sized>) -> anyhow::Result<Response<Full<Bytes>>> {
        let encoder = TextEncoder::new();
        let mut buffer = Vec::with_capacity(4096);
        let metrics = get_metrics_registry().gather();
        encoder.encode(&metrics, &mut buffer)?;
        let response = Response::builder()
            .status(200)
            .header(CONTENT_TYPE, encoder.format_type())
            .body(Full::new(Bytes::from(buffer)))?;
        Ok(response)
    }
}
