//! The MCP stubs through riverbeam's own MCP client: tools list and call,
//! the allowlist, and the credit server's bureau key.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use beam_tools::{McpClient, ToolRegistry};
use showcase::loan_desk::personas::BOB;

fn args(value: &serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().cloned().unwrap_or_default()
}

async fn serve(key: &str) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let router = showcase::loan_desk::mcp_stub::router(key);
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

#[tokio::test]
async fn rates_and_credit_answer_through_the_mcp_client() {
    let base = serve("secret").await;
    let rates = Arc::new(
        McpClient::connect_http("rates", format!("{base}/rates/mcp"), HashMap::new())
            .await
            .unwrap(),
    );
    let registry = ToolRegistry::new();
    let allow: HashSet<String> = ["get_rate_sheet".to_owned()].into();
    let names: Vec<_> = registry
        .register_mcp_server(&rates, Some(&allow))
        .into_iter()
        .map(|d| d.name)
        .collect();
    assert_eq!(names, ["rates__get_rate_sheet"]);
    let sheet = rates
        .call_tool(
            "get_rate_sheet",
            args(&serde_json::json!({"program_id": "va"})),
            None,
        )
        .await
        .unwrap();
    assert!(sheet.contains("5.875"), "{sheet}");

    let keyed = HashMap::from([("x-bureau-key".to_owned(), "secret".to_owned())]);
    let credit = McpClient::connect_http("credit", format!("{base}/credit/mcp"), keyed)
        .await
        .unwrap();
    let report = credit
        .call_tool(
            "pull_report",
            args(&serde_json::json!({"borrower_id": BOB})),
            None,
        )
        .await
        .unwrap();
    assert!(report.contains("641"), "{report}");

    let unkeyed = McpClient::connect_http("credit", format!("{base}/credit/mcp"), HashMap::new())
        .await
        .unwrap();
    let refused = unkeyed
        .call_tool(
            "pull_report",
            args(&serde_json::json!({"borrower_id": BOB})),
            None,
        )
        .await;
    assert!(
        refused.is_err() || refused.as_ref().unwrap().contains("bureau key"),
        "{refused:?}"
    );
}
