//! `showcase-worker`: runs every showcase's agents from the work queue.
//!
//! - `showcase-worker migrate`: Beam's migrations and every showcase's tables.
//! - `showcase-worker seed`: each showcase's demo data, where it is missing.
//! - no argument: run the queue until Ctrl+C.

use showcase::host::queue::start_queue;
use showcase::host::{connect_store, run_migrations};
use showcase::registry::ALL;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    match std::env::args().nth(1).as_deref() {
        Some("migrate") => {
            let applied = run_migrations().await?;
            let store = connect_store().await?;
            for showcase in &ALL {
                showcase.migrate(&store).await?;
            }
            println!("beam migrations applied: {applied:?}; showcase tables ready");
            Ok(())
        }
        Some("seed") => {
            let store = connect_store().await?;
            for showcase in &ALL {
                showcase.migrate(&store).await?;
                showcase.seed(&store).await?;
            }
            Ok(())
        }
        _ => {
            let store = connect_store().await?;
            let (_beam, queue) = start_queue(store).await?;
            println!("showcase worker running the work queue");
            tokio::signal::ctrl_c().await?;
            queue.stop().await
        }
    }
}
