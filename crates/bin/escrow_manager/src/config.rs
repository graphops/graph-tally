use std::collections::BTreeMap;

use alloy::primitives::{Address, B256};
use reqwest::Url;
use serde::Deserialize;
use serde_with::serde_as;

#[serde_as]
#[derive(Deserialize)]
pub struct Config {
    /// Authorize signers on startup.
    pub authorize_signers: bool,
    /// Skip contract calls (for testing/debugging).
    #[serde(default)]
    pub dry_run: bool,
    /// Table of minimum debts by indexer. This can be used, for example, to account for receipts
    /// missing from the kafka topic.
    pub debts: BTreeMap<Address, u64>,
    /// PaymentsEscrow contract address
    pub payments_escrow_contract: Address,
    /// GraphTallyCollector contract address
    pub graph_tally_collector_contract: Address,
    /// GRT contract for updating allowance
    pub grt_contract: Address,
    /// GRT allowance to set on startup
    pub grt_allowance: u64,
    /// Kafka configuration
    pub kafka: Kafka,
    /// Graph network subgraph URL
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub network_subgraph: Url,
    /// Optional API key for querying subgraphs. Missing, null, or empty means no Authorization header.
    #[serde(default, deserialize_with = "deserialize_query_auth")]
    pub query_auth: Option<String>,
    /// RPC for executing transactions
    #[serde_as(as = "serde_with::DisplayFromStr")]
    pub rpc_url: Url,
    /// Secret key of the Graph Tally payer wallet
    pub secret_key: B256,
    /// Secret keys of the Graph Tally signer wallets, used to filter the indexer fees messages.
    pub signers: Vec<B256>,
    /// Period of the subgraph polling cycle
    pub update_interval_seconds: u32,
    /// Port for metrics server
    #[serde(default = "default_port_metrics")]
    pub port_metrics: u16,
    /// Fraction of a receiver's target escrow balance that its debt is allowed to reach before the
    /// balance is raised to the next step. This sets the funding margin: the target balance settles
    /// at roughly `debt / balance_fill_factor`, so 0.6 funds ~1.67x debt and 0.8 funds ~1.25x.
    /// Must be in the range (0, 1].
    #[serde(default = "default_balance_fill_factor")]
    pub balance_fill_factor: f64,
    /// Reclaim escrow that sits above a receiver's target balance. When disabled the manager only
    /// ever deposits, and a receiver's balance is a high-water mark of its past debt. Reclaiming
    /// takes effect via `thaw` and `withdraw`, so funds only return after the escrow contract's
    /// thawing period (28 days on mainnet) has elapsed.
    #[serde(default)]
    pub withdraw_enabled: bool,
    /// Headroom kept above the target balance before any escrow is reclaimed: escrow is only
    /// thawed above `target * (1 + withdraw_margin)`. This absorbs debt growth over the thawing
    /// period and provides hysteresis against the deposit step ladder, so it should comfortably
    /// exceed the debt growth expected for a single receiver over that period. Must be in the
    /// range [0, 1].
    #[serde(default = "default_withdraw_margin")]
    pub withdraw_margin: f64,
    /// Minimum excess, in whole GRT, required to start thawing a receiver's escrow. Excess below
    /// this is left alone, since reclaiming it is not worth the thawing period. It gates starting a
    /// thaw only: a thaw already running is never resized, so the threshold is not reapplied to it.
    #[serde(default = "default_min_withdraw_grt")]
    pub min_withdraw_grt: u64,
}

fn deserialize_query_auth<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.filter(|token| !token.is_empty()))
}

fn default_port_metrics() -> u16 {
    9090
}

fn default_balance_fill_factor() -> f64 {
    0.8
}

fn default_withdraw_margin() -> f64 {
    0.25
}

fn default_min_withdraw_grt() -> u64 {
    500
}

#[derive(Debug, Deserialize)]
pub struct Kafka {
    pub config: BTreeMap<String, String>,
    pub realtime_topic: String,
    pub aggregated_topic: Option<String>,
    /// Cutoff timestamp (unix milliseconds) for aggregated topic data.
    /// Aggregated records older than this are ignored.
    pub aggregated_cutoff_timestamp: Option<i64>,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{http::HeaderMap, routing::post, Json, Router};
    use serde_json::{json, Value};
    use thegraph_client_subgraphs::Client;

    use super::Config;

    fn config_json(query_auth: Option<Value>) -> Value {
        let mut config = json!({
            "authorize_signers": true,
            "debts": {},
            "payments_escrow_contract": "0x0000000000000000000000000000000000000000",
            "graph_tally_collector_contract": "0x0000000000000000000000000000000000000000",
            "grt_contract": "0x0000000000000000000000000000000000000000",
            "grt_allowance": 0,
            "kafka": {"config": {}, "realtime_topic": "receipts"},
            "network_subgraph": "http://127.0.0.1/subgraph",
            "rpc_url": "http://127.0.0.1/rpc",
            "secret_key": "0x0000000000000000000000000000000000000000000000000000000000000001",
            "signers": [],
            "update_interval_seconds": 30
        });
        if let Some(query_auth) = query_auth {
            config["query_auth"] = query_auth;
        }
        config
    }

    #[test]
    fn query_auth_accepts_unauthenticated_and_authenticated_configs() {
        for value in [None, Some(Value::Null), Some(json!(""))] {
            let config: Config = serde_json::from_value(config_json(value)).unwrap();
            assert_eq!(config.query_auth, None);
        }
        let config: Config =
            serde_json::from_value(config_json(Some(json!("test-token")))).unwrap();
        assert_eq!(config.query_auth.as_deref(), Some("test-token"));
    }

    #[test]
    fn query_auth_rejects_non_string_tokens() {
        for value in [json!(false), json!(42), json!([]), json!({})] {
            assert!(serde_json::from_value::<Config>(config_json(Some(value))).is_err());
        }
    }

    #[tokio::test]
    async fn query_auth_controls_actual_request_headers_including_rebuilt_clients() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let app = Router::new().route(
            "/subgraph",
            post(move |headers: HeaderMap| {
                let captured = captured.clone();
                async move {
                    captured.lock().unwrap().push(
                        headers
                            .get("authorization")
                            .map(|value| value.to_str().unwrap().to_owned()),
                    );
                    Json(json!({"data": {"payer": {"signers": []}}}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/subgraph", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();

        for (value, expected) in [
            (None, None),
            (Some(Value::Null), None),
            (Some(json!("")), None),
            (Some(json!("test-token")), Some("Bearer test-token")),
        ] {
            let mut config: Config = serde_json::from_value(config_json(value)).unwrap();
            config.network_subgraph = url.parse().unwrap();
            let mut client = Client::builder(http.clone(), config.network_subgraph)
                .with_auth_token(config.query_auth.clone())
                .build();
            for _ in 0..2 {
                let signers = crate::subgraphs::authorized_signers(
                    &mut client,
                    &alloy::primitives::Address::ZERO,
                )
                .await
                .unwrap();
                assert!(signers.is_empty());
                assert_eq!(
                    observed.lock().unwrap().last().unwrap().as_deref(),
                    expected,
                );
                client = Client::builder(client.http_client, client.subgraph_url)
                    .with_auth_token(config.query_auth.clone())
                    .with_subgraph_latest_block(123)
                    .build();
            }
        }
        assert_eq!(observed.lock().unwrap().len(), 8);
        server.abort();
    }
}
