//! Bounded shutdown for a server that may run the work queue too.
//!
//! On Ctrl+C riverbase's `serve` drains open connections with no limit, and a
//! browser keeps an RTC SSE stream (and maybe a voice WebSocket) open for as
//! long as its tab is, so the server would never stop. riverbase also catches
//! SIGINT, so a second Ctrl+C would not kill the process either.

use std::error::Error;
use std::future::Future;
use std::time::Duration;

use riverbase_http::web::RiverbaseApp;

use crate::host::queue::RunningQueue;

/// How long shutdown waits for open connections, and then for the queue.
pub const SHUTDOWN_DRAIN: Duration = Duration::from_secs(10);

/// Serve `app` until shutdown, then stop `queue`. After Ctrl+C, open
/// connections get [`SHUTDOWN_DRAIN`] to close and the queue as long again
/// to finish its running tasks (one still running is left to its lease; the
/// sweep requeues it). SIGTERM stops at once, and a second Ctrl+C exits.
///
/// # Errors
/// The server failed, or the queue's shutdown did.
pub async fn serve_until_shutdown(
    app: RiverbaseApp,
    queue: Option<RunningQueue>,
) -> Result<(), Box<dyn Error>> {
    let served = bounded(Box::pin(app.serve()), shutdown_deadline()).await;
    if served.is_none() {
        eprintln!(
            "shutdown: open connections did not close in {}s; stopping anyway",
            SHUTDOWN_DRAIN.as_secs()
        );
    }
    if let Some(queue) = queue {
        match tokio::time::timeout(SHUTDOWN_DRAIN, queue.stop()).await {
            Ok(stopped) => stopped?,
            Err(_) => eprintln!(
                "shutdown: the work queue did not stop in {}s; leaving it",
                SHUTDOWN_DRAIN.as_secs()
            ),
        }
    }
    if let Some(served) = served {
        served?;
    }
    Ok(())
}

/// `work`'s output, or `None` when `deadline` resolves first.
async fn bounded<T>(
    work: impl Future<Output = T>,
    deadline: impl Future<Output = ()>,
) -> Option<T> {
    tokio::select! {
        out = work => Some(out),
        () = deadline => None,
    }
}

/// Resolves [`SHUTDOWN_DRAIN`] after Ctrl+C, or at once on SIGTERM (which
/// riverbase does not handle). A second Ctrl+C exits the process.
async fn shutdown_deadline() {
    use tokio::signal::unix::{signal, SignalKind};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        return std::future::pending().await;
    };
    tokio::select! {
        _ = int.recv() => {
            tokio::spawn(async move {
                // `int` keeps SIGINT caught, so one more Ctrl+C lands here.
                int.recv().await;
                std::process::exit(130);
            });
            tokio::time::sleep(SHUTDOWN_DRAIN).await;
        }
        _ = term.recv() => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_server_that_never_drains_is_cut_off_at_the_deadline() {
        let never = std::future::pending::<()>();
        let deadline = tokio::time::sleep(Duration::from_millis(10));
        assert_eq!(bounded(never, deadline).await, None);
    }

    #[tokio::test]
    async fn a_server_that_drains_returns_its_result() {
        let deadline = std::future::pending::<()>();
        assert_eq!(bounded(async { 7 }, deadline).await, Some(7));
    }
}
