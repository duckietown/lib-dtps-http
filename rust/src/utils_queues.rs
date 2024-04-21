use tokio::sync::broadcast;
use tokio::sync::{broadcast as tokio_broadcast, mpsc as tokio_mpsc};

use crate::debug_with_info;

// use tokio::sync::tokio_broadcast::Receiver as BroadcastReceiver;

pub async fn wrap_recv<T>(r: &mut tokio_broadcast::Receiver<T>) -> Option<T>
where
    T: Clone,
{
    loop {
        match r.recv().await {
            Ok(x) => return Some(x),
            Err(e) => match e {
                tokio_broadcast::error::RecvError::Closed => return None,
                tokio_broadcast::error::RecvError::Lagged(_) => {
                    debug_with_info!("lagged");
                    continue;
                }
            },
        };
    }
}
//
//
// pub async fn wrap_recv2<T>(r: &mut tokio_mpsc::Receiver<T>) -> Option<T>
// where
//     T: Clone,
// {
//     loop {
//         match r.recv().await {
//             Ok(x) => return Some(x),
//             Err(e) => match e {
//                 RecvError::Closed => return None,
//                 RecvError::Lagged(_) => {
//                     debug_with_info!("lagged");
//                     continue;
//                 }
//             },
//         };
//     }
// }
