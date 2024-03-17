use crate::server_state::SavedBlob;
use crate::utils_time::time_nanos_i64;
use crate::{error_with_info, warn_with_info, DTPSError, DTPSR};
use bytes::Bytes;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
#[derive(Debug)]
pub struct BlobManager {
    pub blobs: HashMap<String, SavedBlob>,
    pub blobs_forgotten: HashMap<String, i64>,
}

impl BlobManager {
    pub fn new() -> Self {
        BlobManager {
            blobs: HashMap::new(),
            blobs_forgotten: HashMap::new(),
        }
    }

    pub fn guarantee_blob_exists(&mut self, digest: &str, seconds: f64) {
        // debug_with_info!("Guarantee blob {digest} exists for {seconds} seconds more");
        let now = time_nanos_i64();
        let deadline = now + (seconds * 1_000_000_000.0) as i64;
        match self.blobs.get_mut(digest) {
            None => {
                error_with_info!("Blob {digest} not found");
            }
            Some(sb) => {
                sb.deadline = max(sb.deadline, deadline);
            }
        }
    }
    pub fn cleanup_blobs(&mut self) {
        let now = time_nanos_i64();
        let mut todrop = Vec::new();
        for (digest, sb) in self.blobs.iter() {
            let no_one_needs_it = sb.who_needs_it.is_empty();
            let deadline_passed = now > sb.deadline;
            if no_one_needs_it && deadline_passed {
                todrop.push(digest.clone());
            }
        }
        for digest in todrop {
            // debug_with_info!("Dropping blob {digest} because deadline passed");
            self.blobs.remove(&digest);

            self.blobs_forgotten.insert(digest, now);
        }
    }
    pub fn release_blob(&mut self, digest: &str, who: &str, i: usize) {
        // log::debug!("Del blob {digest} for {who:?}");

        match self.blobs.get_mut(digest) {
            None => {
                if self.blobs_forgotten.contains_key(digest) {
                    warn_with_info!("Blob {digest} to forget already forgotten (who = {who}).");
                } else {
                    error_with_info!("Blob {digest} to forget is completely unknown.");
                }
            }
            Some(sb) => {
                let who_i = (who.to_string(), i);
                if sb.who_needs_it.contains(&who_i) {
                    sb.who_needs_it.remove(&who_i);
                } else {
                    warn_with_info!("Blob {digest} to forget was not needed by {who}");
                }

                if sb.who_needs_it.is_empty() {
                    let now = time_nanos_i64();
                    let deadline_passed = now > sb.deadline;
                    if deadline_passed {
                        self.blobs.remove(digest);
                        self.blobs_forgotten.insert(digest.to_string(), now);
                    }
                }
            }
        }
    }

    pub fn save_blob(&mut self, digest: &str, content: &[u8], who: &str, i: usize, comment: &str) {
        let _ = comment;
        // log::debug!("Add blob {digest} for {who:?}: {comment}");
        let who_i = (who.to_string(), i);
        match self.blobs.get_mut(digest) {
            None => {
                let mut sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    deadline: 0,
                };

                sb.who_needs_it.insert(who_i);
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(sb) => {
                sb.who_needs_it.insert(who_i);
            }
        }
    }

    pub fn save_blob_for_time(&mut self, digest: &str, content: &[u8], delta_seconds: f64) {
        let time_nanos_delta = (delta_seconds * 1_000_000_000.0) as i64;
        let nanos_now = time_nanos_i64();
        let deadline = nanos_now + time_nanos_delta;
        match self.blobs.get_mut(digest) {
            None => {
                let sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    deadline,
                };
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(sb) => {
                sb.deadline = max(sb.deadline, deadline);
            }
        }
    }

    pub fn get_blob(&self, digest: &str) -> Option<&Vec<u8>> {
        return self.blobs.get(digest).map(|v| &v.content);
    }

    pub fn get_blob_bytes(&self, digest: &str) -> DTPSR<Bytes> {
        let x = self.blobs.get(digest).map(|v| Bytes::from(v.content.clone()));
        match x {
            Some(v) => Ok(v),
            None => match self.blobs_forgotten.get(digest) {
                Some(ts) => {
                    let now = time_nanos_i64();
                    let delta = now - ts;
                    let seconds = delta as f64 / 1_000_000_000.0;
                    let msg = format!("Blob {:#?} not found. It was forgotten {} seconds ago", digest, seconds);
                    Err(DTPSError::NotAvailable(msg))
                }
                None => {
                    let msg = format!("Blob {:#?} was never saved", digest);
                    Err(DTPSError::ResourceNotFound(msg)) // should be resource not found
                }
            },
        }
    }
}
