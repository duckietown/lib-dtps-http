use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

use crate::{internal_assertion, DTPSError, DTPSR};

#[derive(Debug, Clone)]
pub struct SharedStatusNotification {
    desc: String,
    notifier: Arc<Notify>,
    shared_state: Arc<Mutex<Option<(bool, String)>>>,
}

impl SharedStatusNotification {
    pub fn new(desc: &str) -> Self {
        Self {
            desc: desc.to_string(),
            notifier: Arc::new(Notify::new()),
            shared_state: Arc::new(Mutex::new(None)),
        }
    }

    pub async fn wait(&self) -> DTPSR<()> {
        self.notifier.notified().await;
        let l = self.shared_state.lock().await;

        return if l.is_some() {
            let (status, msg) = &l.as_ref().unwrap();
            if *status {
                Ok(())
            } else {
                DTPSError::other(msg)
            }
        } else {
            DTPSError::internal_assertion("No status set yet.")
        };
    }

    pub async fn notify(&self, status: bool, msg: &str) -> DTPSR<()> {
        let msg = msg.to_string();
        {
            let mut l = self.shared_state.lock().await;
            if l.is_some() {
                let desc = self.desc.clone();
                let (s, m) = l.as_ref().unwrap();
                return internal_assertion!(
                    "For {desc}, status already set  ({s}: {m}) while setting {status} ({msg})"
                );
            }
            // match l {
            //     Some((s, m)) => {
            //         let desc = self.desc.clone();
            //         return internal_assertion!(
            //         "For {desc}, status already set  ({s}: {m}) while setting {status} ({msg})"
            //
            //     )
            //     }
            //     None => {}
            // }

            *l = Some((status, msg));
        }
        self.notifier.notify_waiters();
        Ok(())
    }
}
