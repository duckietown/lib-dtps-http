use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver as BroadcastReceiver;

use crate::debug_with_info;

pub async fn wrap_recv<T>(r: &mut BroadcastReceiver<T>) -> Option<T>
where
    T: Clone,
{
    loop {
        match r.recv().await {
            Ok(x) => return Some(x),
            Err(e) => match e {
                RecvError::Closed => return None,
                RecvError::Lagged(_) => {
                    debug_with_info!("lagged");
                    continue;
                }
            },
        };
    }
}
