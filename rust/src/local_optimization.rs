/*
Local Publishing Optimization for Rust

This module provides optimizations for local data publishing in Rust,
including connection pooling and local detection mechanisms.
*/

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use once_cell::sync::Lazy;

use crate::connections::TypeOfConnection;
use crate::{RawData, DataSaved, DTPSR, DTPSError};

#[derive(Debug, Clone)]
pub struct LocalOptimizationStats {
    pub local_publishes: u64,
    pub remote_publishes: u64,
    pub bytes_saved: u64,
    pub latency_saved_ns: u128,
}

impl Default for LocalOptimizationStats {
    fn default() -> Self {
        Self {
            local_publishes: 0,
            remote_publishes: 0,
            bytes_saved: 0,
            latency_saved_ns: 0,
        }
    }
}

#[derive(Debug)]
pub struct ConnectionOptimizer {
    // Cache frequently used connections to avoid re-establishing
    connection_cache: HashMap<String, (Instant, Arc<hyper::Client<hyper::client::HttpConnector>>)>,
    stats: LocalOptimizationStats,
    cache_ttl: Duration,
}

impl ConnectionOptimizer {
    pub fn new() -> Self {
        Self {
            connection_cache: HashMap::new(),
            stats: LocalOptimizationStats::default(),
            cache_ttl: Duration::from_secs(300), // 5 minutes TTL
        }
    }

    pub fn get_or_create_client(&mut self, connection_key: &str) -> Arc<hyper::Client<hyper::client::HttpConnector>> {
        let now = Instant::now();
        
        // Clean up expired entries
        self.connection_cache.retain(|_, (timestamp, _)| {
            now.duration_since(*timestamp) < self.cache_ttl
        });
        
        // Get or create client
        if let Some((timestamp, client)) = self.connection_cache.get(connection_key) {
            if now.duration_since(*timestamp) < self.cache_ttl {
                return client.clone();
            }
        }
        
        // Create new client with optimized settings for local connections
        let client = Arc::new(
            hyper::Client::builder()
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(10)
                .http2_keep_alive_interval(Some(Duration::from_secs(30)))
                .http2_keep_alive_timeout(Duration::from_secs(10))
                .build_http()
        );
        
        self.connection_cache.insert(connection_key.to_string(), (now, client.clone()));
        client
    }

    pub fn is_local_connection(con: &TypeOfConnection) -> bool {
        match con {
            TypeOfConnection::UNIX(_) => true,
            TypeOfConnection::TCP(url) => {
                if let Some(host) = url.host_str() {
                    matches!(host, "localhost" | "127.0.0.1" | "::1")
                } else {
                    false
                }
            }
            _ => false,
        }
    }

    pub fn record_local_optimization(&mut self, data_size: usize, latency_saved: Duration) {
        self.stats.local_publishes += 1;
        self.stats.bytes_saved += data_size as u64 * 2; // Avoid double serialization
        self.stats.latency_saved_ns += latency_saved.as_nanos();
    }

    pub fn record_remote_publish(&mut self) {
        self.stats.remote_publishes += 1;
    }

    pub fn get_stats(&self) -> &LocalOptimizationStats {
        &self.stats
    }
}

// Global optimizer instance
static GLOBAL_OPTIMIZER: Lazy<Arc<Mutex<ConnectionOptimizer>>> = 
    Lazy::new(|| Arc::new(Mutex::new(ConnectionOptimizer::new())));

/// Optimized publish function that uses connection pooling and local optimizations
pub async fn optimized_publish(con: &TypeOfConnection, data: &RawData) -> DTPSR<DataSaved> {
    let start_time = Instant::now();
    let is_local = ConnectionOptimizer::is_local_connection(con);
    
    // For local connections, use optimized client with connection pooling
    if is_local {
        let connection_key = con.to_url_repr();
        let client = {
            let mut optimizer = GLOBAL_OPTIMIZER.lock().unwrap();
            optimizer.get_or_create_client(&connection_key)
        };
        
        let result = optimized_post_data_with_client(con, data, client).await;
        
        if result.is_ok() {
            let latency_saved = Duration::from_millis(1); // Minimum 1ms saved from connection reuse
            let mut optimizer = GLOBAL_OPTIMIZER.lock().unwrap();
            optimizer.record_local_optimization(data.content.len(), latency_saved);
        }
        
        result
    } else {
        // Use standard publish for remote connections
        let mut optimizer = GLOBAL_OPTIMIZER.lock().unwrap();
        optimizer.record_remote_publish();
        drop(optimizer);
        
        crate::client_publish::publish(con, data).await
    }
}

async fn optimized_post_data_with_client(
    con: &TypeOfConnection,
    data: &RawData,
    client: Arc<hyper::Client<hyper::client::HttpConnector>>,
) -> DTPSR<DataSaved> {
    use hyper::{Body, Request, Method};
    use crate::utils_headers::{put_header_content_type};
    
    let use_url = match con {
        TypeOfConnection::TCP(url) => url.to_string(),
        TypeOfConnection::UNIX(uc) => {
            let h = hex::encode(&uc.socket_name);
            format!("unix://{}{}", h, uc.path)
        }
        _ => return Err(DTPSError::Other("Unsupported connection type for optimization".to_string())),
    };

    let mut req = Request::builder()
        .method(Method::POST)
        .uri(&use_url)
        .body(Body::from(data.content.clone()))?;

    put_header_content_type(req.headers_mut(), &data.content_type);

    let resp = client.request(req).await
        .map_err(|e| DTPSError::Other(format!("Request failed: {}", e)))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = hyper::body::to_bytes(resp.into_body()).await?;
        let body_text = String::from_utf8_lossy(&body);
        return Err(DTPSError::FailedRequest(
            use_url,
            status.as_u16(),
            status.to_string(),
            body_text.to_string(),
        ));
    }

    let content = hyper::body::to_bytes(resp.into_body()).await?;
    let response_data = RawData::new(content, "application/json", None);
    
    let ds = response_data.interpret_owned::<DataSaved>()
        .map_err(|e| DTPSError::Other(format!("Cannot interpret response: {}", e)))?;
    
    Ok(ds)
}

/// Get optimization statistics
pub fn get_optimization_stats() -> LocalOptimizationStats {
    let optimizer = GLOBAL_OPTIMIZER.lock().unwrap();
    optimizer.get_stats().clone()
}

/// Enable batch publishing for multiple data items to the same topic
pub async fn batch_publish(
    con: &TypeOfConnection, 
    data_items: Vec<RawData>
) -> DTPSR<Vec<DataSaved>> {
    if data_items.is_empty() {
        return Ok(vec![]);
    }

    // For local connections with multiple items, try to batch them
    if ConnectionOptimizer::is_local_connection(con) && data_items.len() > 1 {
        // Use a single connection for all publishes
        let connection_key = con.to_url_repr();
        let client = {
            let mut optimizer = GLOBAL_OPTIMIZER.lock().unwrap();
            optimizer.get_or_create_client(&connection_key)
        };

        let mut results = Vec::with_capacity(data_items.len());
        
        for data in data_items {
            match optimized_post_data_with_client(con, &data, client.clone()).await {
                Ok(result) => results.push(result),
                Err(e) => return Err(e),
            }
        }
        
        Ok(results)
    } else {
        // Fall back to individual publishes
        let mut results = Vec::with_capacity(data_items.len());
        
        for data in data_items {
            match optimized_publish(con, &data).await {
                Ok(result) => results.push(result),
                Err(e) => return Err(e),
            }
        }
        
        Ok(results)
    }
}