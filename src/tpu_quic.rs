use quinn::{ClientConfig, Endpoint};
use rustls::client::danger::{ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig as RustlsClientConfig, Error as RustlsError, NamedGroup};
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_keypair::{Keypair, Signer};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Mandatory Agave TPU QUIC Application-Layer Protocol Negotiation (ALPN) identifier.
pub const ALPN_TPU_PROTOCOL_ID: &[u8] = b"solana-tpu";

/// Number of striped local UDP QUIC endpoints with distinct Ed25519 X.509 client certificates.
/// Multiplies unstaked per-identity QUIC stream quotas by 4x at zero cost.
pub const QUIC_STRIPED_ENDPOINTS: usize = 4;

/// Generates an Agave-compliant self-signed X.509 v3 Ed25519 client certificate and PKCS#8 v1 private key.
/// Required by Agave QUIC TPU listeners to index peer connections and enable 0-RTT early data.
pub fn new_tpu_x509_client_certificate(
    keypair: &Keypair,
) -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    const PKCS8_PREFIX: [u8; 16] = [
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    let mut key_pkcs8_der = Vec::with_capacity(48);
    key_pkcs8_der.extend_from_slice(&PKCS8_PREFIX);
    key_pkcs8_der.extend_from_slice(keypair.secret_bytes());

    let mut cert_der = Vec::with_capacity(0xf4);
    cert_der.extend_from_slice(&[
        0x30, 0x81, 0xf6, 0x30, 0x81, 0xa9, 0xa0, 0x03, 0x02, 0x01, 0x02, 0x02, 0x08, 0x01, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x30, 0x16,
        0x31, 0x14, 0x30, 0x12, 0x06, 0x03, 0x55, 0x04, 0x03, 0x0c, 0x0b, 0x53, 0x6f, 0x6c, 0x61,
        0x6e, 0x61, 0x20, 0x6e, 0x6f, 0x64, 0x65, 0x30, 0x20, 0x17, 0x0d, 0x37, 0x30, 0x30, 0x31,
        0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x5a, 0x18, 0x0f, 0x34, 0x30, 0x39, 0x36,
        0x30, 0x31, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x5a, 0x30, 0x00, 0x30, 0x2a,
        0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
    ]);
    cert_der.extend_from_slice(&keypair.pubkey().to_bytes());
    cert_der.extend_from_slice(&[
        0xa3, 0x29, 0x30, 0x27, 0x30, 0x17, 0x06, 0x03, 0x55, 0x1d, 0x11, 0x01, 0x01, 0xff, 0x04,
        0x0d, 0x30, 0x0b, 0x82, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73, 0x74, 0x30,
        0x0c, 0x06, 0x03, 0x55, 0x1d, 0x13, 0x01, 0x01, 0xff, 0x04, 0x02, 0x30, 0x00, 0x30, 0x05,
        0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x41, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    ]);

    (
        CertificateDer::from(cert_der),
        PrivateKeyDer::try_from(key_pkcs8_der).expect("Valid PKCS#8 Ed25519 key"),
    )
}

/// Formats a validator TPU SocketAddr into Agave's SNI server name (`"{ip}.{port}.sol"`).
/// Ensures Quinn caches per-validator TLS 1.3 session tickets and 0-RTT tokens without collision.
#[inline]
pub fn socket_addr_to_quic_sni(peer: SocketAddr) -> String {
    format!("{}.{}.sol", peer.ip(), peer.port())
}

/// Skip server certificate verification for Solana TPU endpoints (validators use self-signed node certs).
#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, RustlsError> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, RustlsError> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        vec![
            rustls::SignatureScheme::ED25519,
            rustls::SignatureScheme::ECDSA_NISTP256_SHA256,
            rustls::SignatureScheme::RSA_PSS_SHA256,
        ]
    }
}

/// Node network endpoints parsed from `getClusterNodes` RPC.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterNodeInfo {
    pub pubkey: String,
    pub gossip: Option<String>,
    pub tpu: Option<String>,
    pub tpu_quic: Option<String>,
    pub rpc: Option<String>,
    pub version: Option<String>,
}

/// Metric recording a single QUIC TPU stream transmission.
#[derive(Debug, Clone)]
pub struct TpuDispatchMetric {
    pub target_tpu: SocketAddr,
    pub bytes_sent: usize,
    pub rtt_micros: u64,
    pub dispatch_mode: &'static str,
    pub success: bool,
}

/// Summary report for a concurrent multi-leader QUIC fanout dispatch.
#[derive(Debug, Clone)]
pub struct MultiLeaderFanoutSummary {
    pub total_fanout: usize,
    pub successful_dispatches: usize,
    pub parallel_elapsed_micros: u64,
    pub fastest_rtt_micros: u64,
    pub fastest_tpu: Option<SocketAddr>,
    pub metrics: Vec<TpuDispatchMetric>,
}

/// Direct QUIC TPU Transaction Router with Agave `solana-tpu` ALPN,
/// X25519 0-RTT early data, and 4-way striped Ed25519 client identity sockets.
pub struct TpuQuicRouter {
    striped_endpoints: Arc<Vec<Endpoint>>,
    stripe_cursor: Arc<AtomicUsize>,
    rpc_url: String,
    http_client: reqwest::Client,
    node_cache: Arc<RwLock<HashMap<String, SocketAddr>>>,
    forward_node_cache: Arc<RwLock<HashMap<String, SocketAddr>>>,
    connection_pool: Arc<RwLock<HashMap<SocketAddr, quinn::Connection>>>,
}

impl TpuQuicRouter {
    pub fn new(rpc_url: impl Into<String>) -> Result<Self, String> {
        let mut striped_endpoints = Vec::with_capacity(QUIC_STRIPED_ENDPOINTS);

        for _ in 0..QUIC_STRIPED_ENDPOINTS {
            // Restrict key exchange to X25519 to prevent Agave TLS 1.3 HelloRetryRequest round-trips
            let mut provider = rustls::crypto::ring::default_provider();
            provider
                .kx_groups
                .retain(|kx| kx.name() == NamedGroup::X25519);

            let ephemeral_keypair = Keypair::new();
            let (cert, priv_key) = new_tpu_x509_client_certificate(&ephemeral_keypair);

            let mut rustls_config = RustlsClientConfig::builder_with_provider(Arc::new(provider))
                .with_safe_default_protocol_versions()
                .map_err(|e| format!("TLS protocol version error: {:?}", e))?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
                .with_client_auth_cert(vec![cert], priv_key)
                .map_err(|e| format!("Failed to attach TPU client X.509 cert: {:?}", e))?;

            rustls_config.enable_early_data = true;
            rustls_config.alpn_protocols = vec![ALPN_TPU_PROTOCOL_ID.to_vec()];

            let quic_crypto = quinn::crypto::rustls::QuicClientConfig::try_from(rustls_config)
                .map_err(|e| format!("Failed to build QuicClientConfig: {:?}", e))?;
            let mut client_config = ClientConfig::new(Arc::new(quic_crypto));

            // 400ms keep-alive beats Agave's 2s unstaked idle timeout; send_fairness=false flushes single-packet tx atomically
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));
            transport_config.keep_alive_interval(Some(Duration::from_millis(400)));
            transport_config.max_concurrent_uni_streams(1024u32.into());
            transport_config.send_fairness(false);
            client_config.transport_config(Arc::new(transport_config));

            let mut endpoint = Endpoint::client(SocketAddr::from(([0, 0, 0, 0], 0)))
                .map_err(|e| format!("Failed to create striped QUIC endpoint: {}", e))?;
            endpoint.set_default_client_config(client_config);
            striped_endpoints.push(endpoint);
        }

        let http_client = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());

        Ok(Self {
            striped_endpoints: Arc::new(striped_endpoints),
            stripe_cursor: Arc::new(AtomicUsize::new(0)),
            rpc_url: rpc_url.into(),
            http_client,
            node_cache: Arc::new(RwLock::new(HashMap::new())),
            forward_node_cache: Arc::new(RwLock::new(HashMap::new())),
            connection_pool: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// Selects the next striped QUIC endpoint round-robin across the 4 independent client identities.
    #[inline]
    pub fn next_striped_endpoint(&self) -> &Endpoint {
        let idx = self.stripe_cursor.fetch_add(1, Ordering::Relaxed) % self.striped_endpoints.len();
        &self.striped_endpoints[idx]
    }

    pub fn striped_lane_count(&self) -> usize {
        self.striped_endpoints.len()
    }

    /// Fetches and caches validator TPU IP:ports (`tpuQuic` + `tpuForwardsQuic`) via `getClusterNodes`.
    pub async fn refresh_cluster_nodes(&self) -> Result<usize, String> {
        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getClusterNodes"
        });

        let resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("RPC error fetching cluster nodes: {}", e))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| format!("Failed to parse cluster nodes JSON: {}", e))?;

        let mut cache = self.node_cache.write().await;
        let mut fwd_cache = self.forward_node_cache.write().await;
        let mut count = 0;

        if let Some(nodes) = resp.get("result").and_then(|r| r.as_array()) {
            for node in nodes {
                if let Some(pubkey) = node.get("pubkey").and_then(|p| p.as_str()) {
                    let addr_str = node
                        .get("tpuQuic")
                        .and_then(|t| t.as_str())
                        .or_else(|| node.get("tpu_quic").and_then(|t| t.as_str()))
                        .or_else(|| node.get("tpu").and_then(|t| t.as_str()));

                    if let Some(addr) = addr_str {
                        if let Ok(socket_addr) = SocketAddr::from_str(addr) {
                            cache.insert(pubkey.to_string(), socket_addr);
                            count += 1;
                        }
                    }

                    let fwd_str = node
                        .get("tpuForwardsQuic")
                        .and_then(|t| t.as_str())
                        .or_else(|| node.get("tpu_forwards_quic").and_then(|t| t.as_str()));

                    if let Some(f_addr) = fwd_str {
                        if let Ok(socket_addr) = SocketAddr::from_str(f_addr) {
                            fwd_cache.insert(pubkey.to_string(), socket_addr);
                        }
                    }
                }
            }
        }

        info!(
            "Refreshed cluster nodes: cached {} validator TPU endpoints ({} forwarders).",
            count,
            fwd_cache.len()
        );
        Ok(count)
    }

    /// Pre-warms QUIC connections to multiple upcoming leaders in the background.
    pub async fn prewarm_leaders(&self, leader_pubkeys: &[String]) {
        for pubkey in leader_pubkeys {
            self.prewarm_leader(pubkey).await;
        }
    }

    /// Pre-warms QUIC connections (`tpuQuic` and `tpuForwardsQuic`) to an upcoming leader.
    pub async fn prewarm_leader(&self, leader_pubkey: &str) {
        let tpu_addr = {
            let cache = self.node_cache.read().await;
            cache.get(leader_pubkey).cloned()
        };
        let fwd_addr = {
            let fwd_cache = self.forward_node_cache.read().await;
            fwd_cache.get(leader_pubkey).cloned()
        };

        if let Some(addr) = tpu_addr {
            let self_clone = self.clone_handle();
            tokio::spawn(async move {
                if let Ok(_conn) = self_clone.get_or_create_connection(addr).await {
                    debug!("Pre-warmed QUIC tpuQuic connection to leader at {}", addr);
                }
            });
        }
        if let Some(f_addr) = fwd_addr {
            if Some(f_addr) != tpu_addr {
                let self_clone = self.clone_handle();
                tokio::spawn(async move {
                    if let Ok(_conn) = self_clone.get_or_create_connection(f_addr).await {
                        debug!(
                            "Pre-warmed QUIC tpuForwardsQuic connection to leader at {}",
                            f_addr
                        );
                    }
                });
            }
        }
    }

    /// Returns a warm QUIC connection or establishes a new 0-RTT / 1-RTT session using `{ip}.{port}.sol` SNI.
    pub async fn get_or_create_connection(
        &self,
        target_addr: SocketAddr,
    ) -> Result<quinn::Connection, String> {
        let cached = {
            let pool = self.connection_pool.read().await;
            pool.get(&target_addr).cloned()
        };

        if let Some(conn) = cached {
            if conn.close_reason().is_none() {
                return Ok(conn);
            }
            debug!("Evicting closed QUIC connection to {}", target_addr);
            self.connection_pool.write().await.remove(&target_addr);
        }

        let sni = socket_addr_to_quic_sni(target_addr);
        let connecting = self
            .next_striped_endpoint()
            .connect(target_addr, &sni)
            .map_err(|e| {
                format!(
                    "Failed to initiate QUIC connect to {} (SNI {}): {}",
                    target_addr, sni, e
                )
            })?;

        let established = match connecting.into_0rtt() {
            Ok((conn, _zero_rtt_accepted)) => conn,
            Err(connecting_1rtt) => {
                tokio::time::timeout(Duration::from_millis(500), connecting_1rtt)
                    .await
                    .map_err(|_| format!("QUIC connection timeout to {}", target_addr))?
                    .map_err(|e| format!("QUIC handshake failed with {}: {}", target_addr, e))?
            }
        };

        self.connection_pool
            .write()
            .await
            .insert(target_addr, established.clone());
        Ok(established)
    }

    /// Dispatches raw wire transaction bytes to a single TPU socket over a unidirectional QUIC stream.
    pub async fn dispatch_to_addr(
        &self,
        target_addr: SocketAddr,
        raw_tx_bytes: &[u8],
    ) -> Result<TpuDispatchMetric, String> {
        let t0 = Instant::now();
        let mut connection = self.get_or_create_connection(target_addr).await?;

        let mut send_stream = match connection.open_uni().await {
            Ok(s) => s,
            Err(e) => {
                debug!(
                    "QUIC open_uni failed on cached conn to {} ({}), reconnecting...",
                    target_addr, e
                );
                self.connection_pool.write().await.remove(&target_addr);
                connection = self.get_or_create_connection(target_addr).await?;
                connection
                    .open_uni()
                    .await
                    .map_err(|e2| format!("QUIC open_uni retry failed: {}", e2))?
            }
        };

        let write_fut = async {
            send_stream
                .write_all(raw_tx_bytes)
                .await
                .map_err(|e| format!("QUIC write_all error: {}", e))?;
            send_stream
                .finish()
                .map_err(|e| format!("QUIC finish error: {}", e))
        };

        tokio::time::timeout(Duration::from_millis(150), write_fut)
            .await
            .map_err(|_| {
                let _ = self
                    .connection_pool
                    .try_write()
                    .map(|mut p| p.remove(&target_addr));
                format!("QUIC write timeout to {}", target_addr)
            })??;

        let rtt_micros = t0.elapsed().as_micros() as u64;

        Ok(TpuDispatchMetric {
            target_tpu: target_addr,
            bytes_sent: raw_tx_bytes.len(),
            rtt_micros,
            dispatch_mode: "QUIC_TPU_DIRECT",
            success: true,
        })
    }

    /// Dispatches raw wire transaction directly to a leader's cached TPU port.
    pub async fn dispatch_to_leader(
        &self,
        leader_pubkey: &str,
        raw_tx_bytes: &[u8],
    ) -> Result<TpuDispatchMetric, String> {
        let tpu_addr = {
            let cache = self.node_cache.read().await;
            cache.get(leader_pubkey).cloned()
        };

        match tpu_addr {
            Some(addr) => self.dispatch_to_addr(addr, raw_tx_bytes).await,
            None => Err(format!(
                "Leader {} TPU address not found in cluster cache",
                leader_pubkey
            )),
        }
    }

    /// Concurrent Multi-Leader Fanout: dispatches wire transaction bytes simultaneously across
    /// active and upcoming slot leaders over non-blocking unidirectional QUIC streams.
    pub async fn fanout_multi_leader(
        &self,
        leader_pubkeys: &[String],
        raw_tx_bytes: &[u8],
    ) -> MultiLeaderFanoutSummary {
        let t0 = Instant::now();
        let mut addrs = Vec::new();

        {
            let cache = self.node_cache.read().await;
            let fwd_cache = self.forward_node_cache.read().await;
            for (idx, pubkey) in leader_pubkeys.iter().enumerate() {
                if let Some(addr) = cache.get(pubkey) {
                    if !addrs.contains(addr) {
                        addrs.push(*addr);
                    }
                }
                if idx <= 1 {
                    if let Some(fwd_addr) = fwd_cache.get(pubkey) {
                        if !addrs.contains(fwd_addr) {
                            addrs.push(*fwd_addr);
                        }
                    }
                }
            }
        }

        if addrs.is_empty() {
            return MultiLeaderFanoutSummary {
                total_fanout: 0,
                successful_dispatches: 0,
                parallel_elapsed_micros: t0.elapsed().as_micros() as u64,
                fastest_rtt_micros: 0,
                fastest_tpu: None,
                metrics: Vec::new(),
            };
        }

        let mut warm_conns = Vec::new();
        let mut cold_addrs = Vec::new();
        {
            let pool = self.connection_pool.read().await;
            for addr in &addrs {
                if let Some(conn) = pool.get(addr) {
                    if conn.close_reason().is_none() {
                        warm_conns.push((*addr, conn.clone()));
                        continue;
                    }
                }
                cold_addrs.push(*addr);
            }
        }

        let mut metrics = Vec::new();
        let mut successful = 0;
        let mut fastest_rtt = u64::MAX;
        let mut fastest_tpu = None;

        if !warm_conns.is_empty() {
            let mut warm_set = tokio::task::JoinSet::new();
            for (addr, conn) in warm_conns {
                let bytes = raw_tx_bytes.to_vec();
                let pool_ref = self.connection_pool.clone();
                warm_set.spawn(async move {
                    let t_stream = Instant::now();
                    let fut = async {
                        let mut s = conn.open_uni().await.map_err(|e| e.to_string())?;
                        s.write_all(&bytes).await.map_err(|e| e.to_string())?;
                        s.finish().map_err(|e| e.to_string())?;
                        Ok::<(), String>(())
                    };
                    match tokio::time::timeout(Duration::from_millis(4), fut).await {
                        Ok(Ok(())) => Ok::<TpuDispatchMetric, String>(TpuDispatchMetric {
                            target_tpu: addr,
                            bytes_sent: bytes.len(),
                            rtt_micros: t_stream.elapsed().as_micros() as u64,
                            dispatch_mode: "QUIC_0RTT_WARM",
                            success: true,
                        }),
                        _ => {
                            let _ = pool_ref.try_write().map(|mut p| p.remove(&addr));
                            Err("warm stream stalled".to_string())
                        }
                    }
                });
            }

            while let Some(res) = warm_set.join_next().await {
                if let Ok(Ok(metric)) = res {
                    if metric.rtt_micros < fastest_rtt {
                        fastest_rtt = metric.rtt_micros;
                        fastest_tpu = Some(metric.target_tpu);
                    }
                    successful += 1;
                    metrics.push(metric);
                }
            }

            for addr in cold_addrs {
                let router = self.clone_handle();
                let bytes = raw_tx_bytes.to_vec();
                tokio::spawn(async move {
                    let _ = router.dispatch_to_addr(addr, &bytes).await;
                });
            }
        } else {
            let mut join_set = tokio::task::JoinSet::new();
            for addr in cold_addrs {
                let router = self.clone_handle();
                let bytes = raw_tx_bytes.to_vec();
                join_set.spawn(async move { router.dispatch_to_addr(addr, &bytes).await });
            }

            let deadline = Instant::now() + Duration::from_millis(60);
            while let Ok(Some(res)) =
                tokio::time::timeout_at(deadline.into(), join_set.join_next()).await
            {
                if let Ok(Ok(metric)) = res {
                    if metric.rtt_micros < fastest_rtt {
                        fastest_rtt = metric.rtt_micros;
                        fastest_tpu = Some(metric.target_tpu);
                    }
                    successful += 1;
                    metrics.push(metric);
                    tokio::spawn(async move {
                        while join_set.join_next().await.is_some() {}
                    });
                    break;
                }
            }
        }

        MultiLeaderFanoutSummary {
            total_fanout: addrs.len(),
            successful_dispatches: successful,
            parallel_elapsed_micros: t0.elapsed().as_micros() as u64,
            fastest_rtt_micros: if fastest_rtt == u64::MAX { 0 } else { fastest_rtt },
            fastest_tpu,
            metrics,
        }
    }

    pub fn clone_handle(&self) -> Self {
        Self {
            striped_endpoints: self.striped_endpoints.clone(),
            stripe_cursor: self.stripe_cursor.clone(),
            rpc_url: self.rpc_url.clone(),
            http_client: self.http_client.clone(),
            node_cache: self.node_cache.clone(),
            forward_node_cache: self.forward_node_cache.clone(),
            connection_pool: self.connection_pool.clone(),
        }
    }

    pub async fn purge_stale_connections(&self) -> usize {
        let mut pool = self.connection_pool.write().await;
        let before = pool.len();
        pool.retain(|_addr, conn| conn.close_reason().is_none());

        if pool.len() > 32 {
            let excess = pool.len() - 32;
            let keys_to_remove: Vec<_> = pool.keys().take(excess).cloned().collect();
            for key in keys_to_remove {
                pool.remove(&key);
            }
        }

        before.saturating_sub(pool.len())
    }

    pub async fn active_connection_count(&self) -> usize {
        self.connection_pool.read().await.len()
    }

    pub async fn has_warm_connection(&self, addr: &SocketAddr) -> bool {
        let pool = self.connection_pool.read().await;
        pool.get(addr)
            .map(|c| c.close_reason().is_none())
            .unwrap_or(false)
    }

    pub async fn insert_node(&self, pubkey: impl Into<String>, addr: SocketAddr) {
        self.node_cache.write().await.insert(pubkey.into(), addr);
    }

    /// Fallback HTTP JSON-RPC `sendTransaction` (`skipPreflight: true`, `maxRetries: 0`).
    pub async fn broadcast_rpc_fallback(&self, raw_tx_bytes: &[u8]) -> Result<String, String> {
        let tx_base64 =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, raw_tx_bytes);

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                tx_base64,
                {
                    "encoding": "base64",
                    "skipPreflight": true,
                    "preflightCommitment": "processed",
                    "maxRetries": 0
                }
            ]
        });

        let resp = self
            .http_client
            .post(&self.rpc_url)
            .json(&payload)
            .send()
            .await
            .map_err(|e| format!("RPC error ({}): {}", self.rpc_url, e))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| format!("RPC JSON error ({}): {}", self.rpc_url, e))?;

        if let Some(err) = resp.get("error") {
            return Err(format!("RPC error response ({}): {}", self.rpc_url, err));
        }

        resp.get("result")
            .and_then(|r| r.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| format!("Missing signature in RPC response ({})", self.rpc_url))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sni_and_x509_der_certificate_generation() {
        let addr: SocketAddr = "127.0.0.1:8009".parse().unwrap();
        assert_eq!(socket_addr_to_quic_sni(addr), "127.0.0.1.8009.sol");

        let kp = Keypair::new();
        let (cert, _priv_key) = new_tpu_x509_client_certificate(&kp);
        assert!(cert.as_ref().len() > 200);
        assert_eq!(cert.as_ref()[0], 0x30); // ASN.1 SEQUENCE header
    }

    #[tokio::test]
    async fn test_tpu_quic_router_4lane_initialization() {
        let router = TpuQuicRouter::new("https://api.devnet.solana.com")
            .expect("Router must initialize 4 striped QUIC endpoints");
        assert_eq!(router.striped_lane_count(), QUIC_STRIPED_ENDPOINTS);
        assert_eq!(router.purge_stale_connections().await, 0);
    }

    #[tokio::test]
    async fn test_multi_leader_fanout_empty_and_unreachable_timeout() {
        let router = TpuQuicRouter::new("https://api.devnet.solana.com").unwrap();
        let empty = router
            .fanout_multi_leader(&["UnknownLeader1111111111111111111111111111".into()], b"tx")
            .await;
        assert_eq!(empty.total_fanout, 0);
        assert_eq!(empty.successful_dispatches, 0);

        let fake_addr: SocketAddr = "127.0.0.1:59999".parse().unwrap();
        router.insert_node("MockLeader1", fake_addr).await;
        let summary = router
            .fanout_multi_leader(&["MockLeader1".into()], b"dummy_tx_bytes")
            .await;
        assert_eq!(summary.total_fanout, 1);
        assert_eq!(summary.successful_dispatches, 0);
        assert!(summary.parallel_elapsed_micros < 2_000_000);
    }

    #[tokio::test]
    async fn test_prewarm_leaders_async_nonblocking() {
        let router = TpuQuicRouter::new("https://api.devnet.solana.com").unwrap();
        router
            .insert_node("Leader1", "127.0.0.1:59991".parse().unwrap())
            .await;
        router
            .insert_node("Leader2", "127.0.0.1:59992".parse().unwrap())
            .await;

        let t0 = Instant::now();
        router
            .prewarm_leaders(&["Leader1".into(), "Leader2".into()])
            .await;
        assert!(t0.elapsed().as_millis() < 50);
    }
}
