use std::cmp::max;
use std::collections::{HashMap, HashSet};

use bytes::Bytes;

use crate::utils_time::time_nanos_i64;
use crate::{debug_with_info, error_with_info, warn_with_info, DTPSError, DTPSR};

#[derive(Debug, Clone)]
pub struct SavedBlob {
    pub content: Vec<u8>,
    pub who_needs_it: HashSet<(String, usize)>,
    pub outstanding_tokens: HashMap<String, i64>,
}

impl SavedBlob {
    pub fn clean_old(&mut self, now: i64) {
        let mut tokens_to_drop = Vec::new();
        for (token, deadline) in self.outstanding_tokens.iter() {
            if now > *deadline {
                // warn_with_info!("Outstanding token {token} for blob {self.digest} expired");
                tokens_to_drop.push(token.clone());
            }
        }
        for token in tokens_to_drop {
            self.outstanding_tokens.remove(&token);
        }
    }

    pub fn someone_needs_it(&self) -> bool {
        let queue_needs_it = !self.who_needs_it.is_empty();
        let tokens_need_it = !self.outstanding_tokens.is_empty();
        queue_needs_it || tokens_need_it
    }
}

#[derive(Debug)]
pub struct BlobManager {
    pub blobs: HashMap<String, SavedBlob>,
    pub blobs_forgotten: HashMap<String, i64>,
    pub forget_forgetting_interval_s: f32,
    pub last_cleanup: i64,
}

impl BlobManager {
    pub fn new(forget_forgetting_interval_s: f32) -> Self {
        BlobManager {
            blobs: HashMap::new(),
            blobs_forgotten: HashMap::new(),
            forget_forgetting_interval_s,
            last_cleanup: 0,
        }
    }
    pub fn summarize(&self) -> String {
        let mut s = String::new();
        let now = time_nanos_i64();
        s.push_str("BlobManager:\n");
        let mut total_size = 0;
        for sb in self.blobs.values() {
            total_size += sb.content.len();
        }
        s.push_str(&format!("Total Blob size: {total_size}\n"));
        s.push_str("Blobs:\n");
        // order blobs by size
        let mut blobs_sorted = self.blobs.iter().collect::<Vec<_>>();
        blobs_sorted.sort_by(|a, b| a.1.content.len().cmp(&b.1.content.len()));

        for (digest, sb) in blobs_sorted.iter() {
            let mut s_needed = String::new();
            for (who, i) in sb.who_needs_it.iter() {
                s_needed.push_str(&format!(" ['{who}'@{i}] "));
            }
            let outstanding = sb.outstanding_tokens.len();
            let mut max_deadline = now;
            for (_, deadline) in sb.outstanding_tokens.iter() {
                max_deadline = max(max_deadline, *deadline);
            }
            s.push_str(&format!(" {digest}: {} {}", sb.content.len(), s_needed,));
            if outstanding > 0 {
                let delta = max_deadline - now;
                let seconds = delta as f64 / 1_000_000_000.0;
                s.push_str(&format!(" (outstanding: {outstanding} until {seconds}s)"));
            }
            s.push_str("\n");
        }
        s.push_str(&format!("Forgotten blobs: {}\n", self.blobs_forgotten.len()));

        let delta = now - self.last_cleanup;
        let seconds = delta as f64 / 1_000_000_000.0;
        s.push_str(&format!("Last cleanup: {seconds}s ago\n"));
        s
    }
    pub fn cleanup_blobs(&mut self) {
        let now = time_nanos_i64();
        let mut todrop = Vec::new();
        for (digest, sb) in self.blobs.iter_mut() {
            sb.clean_old(now);

            if !sb.someone_needs_it() {
                todrop.push(digest.clone());
            }
        }
        for digest in todrop {
            self.blobs.remove(&digest);

            self.blobs_forgotten.insert(digest, now);
        }
        let mut todrop_memories = Vec::new();
        for (digest, ts) in self.blobs_forgotten.iter() {
            let delta = now - ts;
            let seconds = delta as f64 / 1_000_000_000.0;
            if seconds > self.forget_forgetting_interval_s as f64 {
                todrop_memories.push(digest.clone());
            }
        }
        for digest in todrop_memories {
            self.blobs_forgotten.remove(&digest);
        }
        self.last_cleanup = now;
    }

    pub fn save_blob_for_queue(&mut self, digest: &str, content: &[u8], who: &str, i: usize, comment: &str) {
        let _ = comment;
        // log::debug!("Add blob {digest} for {who:?}: {comment}");
        let who_i = (who.to_string(), i);
        let x = self._save_blob(digest, content);
        x.who_needs_it.insert(who_i);
    }

    pub fn release_blob_for_queue(&mut self, digest: &str, who: &str, i: usize) {
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
                let now = time_nanos_i64();

                sb.clean_old(now);
                if !sb.someone_needs_it() {
                    self.blobs.remove(digest);
                    self.blobs_forgotten.insert(digest.to_string(), now);
                }
            }
        }
    }

    fn _save_blob(&mut self, digest: &str, content: &[u8]) -> &mut SavedBlob {
        match self.blobs.get_mut(digest) {
            None => {
                let sb = SavedBlob {
                    content: content.to_vec(),
                    who_needs_it: HashSet::new(),
                    outstanding_tokens: HashMap::new(),
                };
                self.blobs.insert(digest.to_string(), sb);
            }
            Some(_) => {
                // sb.deadline = 0;
            }
        }
        return self.blobs.get_mut(digest).unwrap();
    }

    pub fn get_blob(&self, digest: &str) -> Option<&Vec<u8>> {
        return self.blobs.get(digest).map(|v| &v.content);
    }

    pub fn get_blob_once(&mut self, digest: &str, token: &str) -> DTPSR<Bytes> {
        let bmut = self.blobs.get_mut(digest);
        match bmut {
            Some(sb) => {
                let now = time_nanos_i64();

                if sb.outstanding_tokens.contains_key(token) {
                    sb.outstanding_tokens.remove(token);
                } else {
                    warn_with_info!("Token {token} not found for blob {digest} but blob ok");
                }
                let b = Bytes::from(sb.content.clone());

                sb.clean_old(now);
                if !(sb.someone_needs_it()) {
                    self.blobs.remove(digest);
                    self.blobs_forgotten.insert(digest.to_string(), now);
                }

                return Ok(b);
            }
            None => {
                let msg = format!("Blob {:#?} not available", digest);
                Err(DTPSError::ResourceNotFound(msg)) // should be resource not found
            }
        }
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
    pub fn get_use_once_link_store(
        &mut self,
        digest: &str,
        content: &[u8],
        content_type: &str,
        max_availability_s: f32,
    ) -> String {
        let a = self.get_use_once_link(digest, Some(content), content_type, max_availability_s);
        return a.unwrap();
    }
    pub fn get_use_once_link(
        &mut self,
        digest: &str,
        content: Option<&[u8]>,
        content_type: &str,
        max_availability_s: f32,
    ) -> DTPSR<String> {
        let x = if let Some(content) = content {
            self._save_blob(digest, content)
        } else {
            if !self.blobs.contains_key(digest) {
                let msg = format!("Blob {digest} not found");
                return Err(DTPSError::NotAvailable(msg));
            }
            self.blobs.get_mut(digest).unwrap()
        };

        let time_nanos_delta = (max_availability_s * 1_000_000_000.0) as i64;
        let nanos_now = time_nanos_i64();
        let deadline = nanos_now + time_nanos_delta;

        let uuid = uuid::Uuid::new_v4().to_string();
        x.outstanding_tokens.insert(uuid.clone(), deadline);

        Ok(format_digest_path(digest, content_type, &uuid))
    }
}

pub fn format_digest_path(digest: &str, content_type: &str, token: &str) -> String {
    format!("!/:ipfs/{}/{}/{}/", digest, content_type.replace("/", "_"), token)
}
