//! `showcase-mcp`: every showcase's outside services as MCP servers, on
//! `SHOWCASE_MCP_ADDR` (default 127.0.0.1:9310). See [`showcase::mcp_stub`].

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("SHOWCASE_MCP_ADDR").unwrap_or_else(|_| "127.0.0.1:9310".into());
    let keys = showcase::mcp_stub::Keys::from_env();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!(
        "showcase MCP stubs on http://{addr} ({})",
        showcase::mcp_stub::PATHS.join(", ")
    );
    axum::serve(listener, showcase::mcp_stub::router(&keys)).await?;
    Ok(())
}
