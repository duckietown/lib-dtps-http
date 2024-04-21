use std::cmp::max;
use std::collections::{HashMap, HashSet, VecDeque};

use bytes::Bytes;
use derive_more::Constructor;

use crate::types::{Digest, ReaderID, Time};
use crate::utils_time::{format_delay, format_delay_s, time_nanos_i64};
use crate::websocket_abstractions::my_base64_encode_str;
use crate::{debug_with_info, error_with_info, warn_with_info, DTPSError, ServerStateAccess, DTPSR};

#[derive(Debug, Clone, Constructor)]
pub struct ReaderLast {
    pub at: Time,
    pub seq: usize,
}

#[derive(Debug, Clone)]
pub struct ReaderEntry {
    pub seq: usize,
    pub entry_added: Time,
    pub entry_deadline: Time,
    pub digest: Digest,
}

#[derive(Debug)]
pub struct ReaderInfo {
    pub started: Time,
    pub count: usize,
    pub nread: usize,
    pub nskipped: usize,
    pub nexpired: usize,
    pub outstanding: VecDeque<ReaderEntry>,
    pub last_read: Option<ReaderLast>,
    pub last_added: Option<ReaderLast>,
    pub last_cleanup: Option<Time>,
    pub debug_history: String,
    pub last_activity: Time,
    pub is_finished: bool,
}

impl ReaderInfo {
    pub fn new(now: Time) -> Self {
        let debug_history = format!("@{}: Created\n", now);
        ReaderInfo {
            started: now,
            count: 0,
            nread: 0,
            nskipped: 0,
            nexpired: 0,
            outstanding: VecDeque::new(),
            last_read: None,
            last_added: None,
            last_cleanup: None,
            last_activity: now,
            debug_history,
            is_finished: false,
        }
    }
    pub fn set_finished(&mut self) {
        self.is_finished = true;
    }
    pub fn is_abandoned(&self, now: Time, max_idle_s: f32) -> bool {
        let delta = now - self.last_activity;
        let seconds = delta as f64 / 1_000_000_000.0;
        seconds > max_idle_s as f64
    }
    pub fn can_be_removed(&self, now: Time, max_idle_s: f32) -> bool {
        (self.is_finished && self.outstanding.len() == 0) || self.is_abandoned(now, max_idle_s)
    }
    pub fn still_outstanding(&self, i: &usize) -> bool {
        if self.outstanding.len() == 0 {
            return false;
        }
        let first = self.outstanding.front().unwrap();
        let last = self.outstanding.back().unwrap();
        first.seq <= *i && *i <= last.seq
    }
    pub fn _comment(&mut self, now: Time, msg: String) {
        let msg = format!("@{}: {}\n", now, msg);

        // self.debug_history.push_str(&msg);
    }
    pub fn add(&mut self, now: Time, deadline: Time, digest: &Digest) -> usize {
        let i = self.count;
        self._comment(now, format!("seq {} points to digest {}", i, digest));

        let e = ReaderEntry {
            seq: i,
            entry_added: now,
            entry_deadline: deadline,
            digest: digest.clone(),
        };
        self.outstanding.push_back(e);
        self.last_added = Some(ReaderLast::new(now, i));
        self.last_activity = now;
        self.count += 1;
        i
    }
    pub fn drop_expired(&mut self, now: Time) -> Vec<(usize, Digest)> {
        let mut todrop = Vec::new();

        while self.outstanding.len() > 0 {
            let first = self.outstanding.front().unwrap();

            if first.entry_deadline < now {
                // let msg = format!("Sequence item {} expired.", first.seq);
                // warn_with_info!("{}", msg);
                // self._comment(now, format!("seq {} -> {} is expired", first.seq, first.digest));
                todrop.push((first.seq, first.digest.clone()));
                self.outstanding.pop_front();
                self.nexpired += 1;
            } else {
                break;
            }
        }
        for (i, digest) in todrop.iter() {
            self._comment(now, format!("seq {} -> {} is dropped because expired", i, digest));
        }
        if todrop.len() == 0 {
            self._comment(now, format!("no items dropped at this time"));
        }
        self.last_cleanup = Some(now);
        todrop
    }
    /// Performs one read to the sequence number `seq` for the reader.
    /// If successful, returns the digest to read, the entries skipped and the entries to remove.
    ///
    pub fn access_seq(&mut self, now: Time, seq: usize) -> AccessResult {
        let previous_last_read = self.last_read.clone();
        let mut entries_skipped: Vec<usize> = Vec::new();
        let mut entries_to_remove: Vec<(usize, Digest)> = Vec::new();

        let mut digest_to_read: Option<Digest> = None;

        while self.outstanding.len() > 0 {
            let first = self.outstanding.pop_front().unwrap();
            if first.seq < seq {
                let msg = format!("Previous sequence item {} skipped by reader.", first.seq);
                warn_with_info!("{}", msg);
                self._comment(
                    now,
                    format!("reader asked {}: so {} -> {} is skipped", seq, first.seq, first.digest),
                );

                entries_skipped.push(first.seq);
                self.nskipped += 1;
                entries_to_remove.push((first.seq, first.digest.clone()));
            } else if first.seq == seq {
                if first.entry_deadline < now {
                    self._comment(
                        now,
                        format!("reader asked {} -> {} but it is expired", seq, first.digest),
                    );
                    let msg = format!("Sequence item {} expired.", first.seq);
                    warn_with_info!("{}", msg);
                    self.nexpired += 1;
                } else {
                    self._comment(
                        now,
                        format!(
                            "reader asked {} -> {} after {} from insertion: marked for removal",
                            seq,
                            first.digest,
                            format_delay(first.entry_added, now)
                        ),
                    );

                    digest_to_read = Some(first.digest.clone());
                    self.last_read = Some(ReaderLast::new(now, seq));
                    self.nread += 1;
                }
                entries_to_remove.push((first.seq, first.digest.clone()));
                break;
            }
            // else if first.seq > seq {
            //     if first.entry_deadline < now {
            //         self._comment(now, format!("reader asked {}: marking future {} for removal because expired", seq,
            //                                    first.seq));
            //
            //         let msg = format!("Future sequence item {} expired.", first.seq);
            //         warn_with_info!("{}", msg);
            //         entries_to_remove.push((first.seq, first.digest.clone()));
            //     }
            // }
        }
        if digest_to_read.is_none() {
            self._comment(now, format!("reader asked {}: not found", seq));
        }
        self.last_cleanup = Some(now);
        self.last_activity = now;
        AccessResult {
            history: self.debug_history.clone(),
            digest_to_read,
            entries_skipped,
            entries_to_remove,
            last_read: previous_last_read,
        }
    }
}

pub struct AccessResult {
    pub history: String,
    pub digest_to_read: Option<Digest>,
    pub entries_skipped: Vec<usize>,
    pub entries_to_remove: Vec<(usize, Digest)>,
    pub last_read: Option<ReaderLast>,
}

type ReaderSeq = (ReaderID, usize);

#[derive(Debug, Clone)]
pub struct SavedBlob {
    pub content: Vec<u8>,
    pub who_needs_it: HashSet<(String, usize)>,
    pub outstanding_readers: HashSet<ReaderSeq>,
}

impl SavedBlob {
    // pub fn clean_old(&mut self, now: Time) {
    //     let mut tokens_to_drop = Vec::new();
    //     for (token, deadline) in self.outstanding_readers.iter() {
    //         if now > *deadline {
    //             warn_with_info!("Outstanding reservation {} : {} for blob expired",
    //                             token.0, token.1);
    //             tokens_to_drop.push(token.clone());
    //         }
    //     }
    //     for token in tokens_to_drop {
    //         self.outstanding_readers.remove(&token);
    //     }
    // }

    pub fn someone_needs_it(&self) -> bool {
        let queue_needs_it = !self.who_needs_it.is_empty();
        let tokens_need_it = !self.outstanding_readers.is_empty();
        queue_needs_it || tokens_need_it
    }

    pub fn release_for_reader(&mut self, reader: &ReaderID, i: usize) -> bool {
        let readerseq = (reader.to_string(), i);
        if self.outstanding_readers.contains(&readerseq) {
            self.outstanding_readers.remove(&readerseq);
            true
        } else {
            error_with_info!("No reservation {reader}:{i} found for this blob");
            false
        }
    }
    pub fn reader_needs_it(&mut self, reader: &ReaderID, i: usize) {
        let readerseq = (reader.clone(), i);
        if self.outstanding_readers.contains(&readerseq) {
            error_with_info!(
                "Reservation {}:{} already exists for this blob",
                readerseq.0,
                readerseq.1
            );
            return;
        }
        self.outstanding_readers.insert(readerseq.clone());
    }
}

#[derive(Debug)]
pub struct BlobManager {
    pub blobs: HashMap<Digest, SavedBlob>,
    pub blobs_forgotten: HashMap<Digest, i64>,
    pub forget_forgetting_interval_s: f32,
    pub last_cleanup: i64,

    pub readers: HashMap<ReaderID, ReaderInfo>,
}

impl BlobManager {
    pub fn new(forget_forgetting_interval_s: f32) -> Self {
        BlobManager {
            blobs: HashMap::new(),
            blobs_forgotten: HashMap::new(),
            forget_forgetting_interval_s,
            last_cleanup: 0,
            readers: HashMap::new(),
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
            let outstanding = sb.outstanding_readers.len();
            // let mut max_deadline = now;
            // for (_, deadline) in sb.outstanding_readers.iter() {
            //     max_deadline = max(max_deadline, *deadline);
            // }
            s.push_str(&format!(" {digest:26}: {:8} {}", sb.content.len(), s_needed,));
            if outstanding > 0 {
                // let delta = max_deadline - now;
                // let seconds = delta as f64 / 1_000_000_000.0;
                s.push_str(&format!(" (outstanding: {outstanding})"));
            }
            s.push_str("\n");
        }
        s.push_str(&format!("Forgotten blobs: {}\n", self.blobs_forgotten.len()));

        for (reader_id, info) in self.readers.iter() {
            s.push_str(&format!("Reader {reader_id}: "));
            let since_last_activity = format_delay_s(info.last_activity, now);
            let since_last_cleanup = format_delay_s(info.last_cleanup.unwrap_or(0), now);
            let since_last_read = info
                .last_read
                .as_ref()
                .map(|x| format_delay_s(x.at, now))
                .unwrap_or("never".to_string());
            let since_last_added = info
                .last_added
                .as_ref()
                .map(|x| format_delay_s(x.at, now))
                .unwrap_or("never".to_string());
            s.push_str(&format!(
                "  finished: {} count: {:5} (read: {:5}  skipped: {:5} expired: {:5} oustanding: {:5}) last activity: {:5} read: {:5} added: {:5} cleanup: {:5}\n",
                info.is_finished,
                info.count,
                info.nread,
                info.nskipped,
                info.nexpired,
                info.outstanding.len(),
                since_last_activity,
                since_last_read,
                since_last_added,
                since_last_cleanup,
            ));
        }
        if self.readers.len() == 0 {
            s.push_str("No readers\n");
        }

        let delta = now - self.last_cleanup;
        let seconds = delta as f64 / 1_000_000_000.0;
        s.push_str(&format!("Last cleanup: {seconds}s ago\n"));
        s
    }
    pub fn cleanup_blobs(&mut self, now: Time) {
        // let now = time_nanos_i64();

        for (reader_id, reader_info) in self.readers.iter_mut() {
            reader_info._comment(now, format!("Cleanup blobs started"));

            let reservations_expired = reader_info.drop_expired(now);

            for (i, entry) in reservations_expired {
                match self.blobs.get_mut(&entry) {
                    Some(sb) => {
                        let ok = sb.release_for_reader(reader_id, i);
                        if !ok {
                            let msg = format!("Blob {entry} did not find reservation for reader {reader_id}:{i}");
                            error_with_info!("{}\n{}", msg, reader_info.debug_history);

                            reader_info._comment(now, msg);
                        } else {
                            reader_info._comment(
                                now,
                                format!("Reservation {reader_id}:{i} for blob {entry} removed successfully"),
                            );
                        }
                    }
                    None => {
                        let msg = format!("Blob {entry} not found");
                        error_with_info!("{}", msg);
                    }
                }
            }
        }

        let mut readers_to_drop = Vec::new();
        for (reader_id, reader_info) in self.readers.iter_mut() {
            if reader_info.can_be_removed(now, 60.0) {
                readers_to_drop.push(reader_id.clone());
            }
        }
        for reader_id in readers_to_drop {
            self.forget_reader(&reader_id);
        }

        for (digest, sb) in self.blobs.iter_mut() {
            let mut reservation_to_drop = Vec::new();

            for (reader_id, i) in sb.outstanding_readers.iter() {
                if !self.readers.contains_key(reader_id) {
                    let msg = format!("Reader {reader_id} not found but reservation active");
                    error_with_info!("{}", msg);
                    reservation_to_drop.push((reader_id.clone(), *i));
                    continue;
                }
                let reader = self.readers.get(reader_id).unwrap();
                if !reader.still_outstanding(i) {
                    // TODO: debug this case
                    // let msg = format!("Blob {digest} was not still needed by {reader_id}:{i}");
                    // error_with_info!("{}", msg);
                    reservation_to_drop.push((reader_id.clone(), *i));
                }
            }
            for (reader_id, i) in reservation_to_drop {
                let k = (reader_id.clone(), i);
                sb.outstanding_readers.remove(&k);
            }
        }

        let mut todrop = Vec::new();
        for (digest, sb) in self.blobs.iter_mut() {
            // sb.clean_old(now);

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

                // sb.clean_old(now);
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
                    outstanding_readers: HashSet::new(),
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
    pub fn unique_reader_id(&mut self) -> ReaderID {
        uuid::Uuid::new_v4().to_string()
    }

    pub fn get_blob_once(&mut self, digest: &Digest, reader_id: &ReaderID, reader_seq: usize) -> DTPSR<Bytes> {
        let now = time_nanos_i64();

        let reader = self
            .readers
            .get_mut(reader_id)
            .ok_or(DTPSError::ResourceNotFound("Reader not found".to_string()))?;

        reader._comment(now, format!("get_blob_once asks for sequence {reader_seq}"));

        let access_result = reader.access_seq(now, reader_seq);

        let digest_to_read = access_result.digest_to_read.ok_or({
            let mut msg = format!("Sequence item {reader_seq} not found for reader.\n");
            msg.push_str("History:\n");
            msg.push_str(&access_result.history);

            // todo: still do clean up here

            DTPSError::ResourceNotFound(msg)
        })?;

        if access_result.entries_skipped.len() > 0 {
            let msg = format!(
                "This reader skipped {} elements of the sequence. Current index: {}.",
                access_result.entries_skipped.len(),
                reader_seq
            );
            warn_with_info!("{}", msg);
        }
        if *digest != digest_to_read {
            let msg = format!("Mismatch between {} and {}", digest, digest_to_read);
            return DTPSError::internal_assertion(msg);
        }

        let bmut = self.blobs.get_mut(&digest_to_read);
        let data = match bmut {
            Some(sb) => Bytes::from(sb.content.clone()),
            None => {
                let msg = format!("Blob {:#?} not available", digest);
                return Err(DTPSError::ResourceNotFound(msg)); // should be resource not found
            }
        };

        for (i, digest) in access_result.entries_to_remove.iter() {
            match self.blobs.get_mut(digest) {
                Some(sb) => {
                    let ok = sb.release_for_reader(reader_id, *i);

                    if !ok {
                        reader._comment(
                            now,
                            format!("Reservation {reader_id}:{i} for blob {digest} was not found!"),
                        );

                        let msg = format!("Blob {digest} not found for reader {reader_id}:{i}");
                        error_with_info!("{}\n{}", msg, reader.debug_history);
                    } else {
                        reader._comment(
                            now,
                            format!("Reservation {reader_id}:{i} for blob {digest} removed successfully"),
                        );
                    }

                    // sb.clean_old(now);
                    if !sb.someone_needs_it() {
                        self.blobs.remove(digest);
                        self.blobs_forgotten.insert(digest.to_string(), now);
                    }
                }
                None => {
                    let msg = format!("Blob {digest} not found");
                    return Err(DTPSError::NotAvailable(msg));
                }
            }
        }

        Ok(data)
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
        digest: &Digest,
        content: &[u8],
        content_type: &str,
        max_availability_s: f32,
        reader_id: &ReaderID,
    ) -> String {
        let a = self.get_use_once_link(digest, Some(content), content_type, max_availability_s, reader_id);
        return a.unwrap();
    }
    pub fn get_use_once_link(
        &mut self,
        digest: &Digest,
        content: Option<&[u8]>,
        content_type: &str,
        max_availability_s: f32,
        reader_id: &ReaderID,
    ) -> DTPSR<String> {
        let now = time_nanos_i64();

        if let Some(content) = content {
            self._save_blob(digest, content);
        } else {
            if !self.blobs.contains_key(digest) {
                let msg = format!("Blob {digest} not found");
                return Err(DTPSError::NotAvailable(msg));
            }
        };

        let time_nanos_delta = (max_availability_s * 1_000_000_000.0) as i64;
        let deadline = now + time_nanos_delta;

        let entry = self.readers.entry(reader_id.clone()).or_insert(ReaderInfo::new(now));

        let i = entry.add(now, deadline, digest);

        let blob = self.blobs.get_mut(digest).unwrap();
        blob.reader_needs_it(reader_id, i);

        Ok(format_digest_path(digest, content_type, reader_id, i))
    }

    pub fn forget_reader(&mut self, reader_id: &ReaderID) {
        if let Some(reader) = self.readers.get_mut(reader_id) {
            let now = time_nanos_i64();
            if reader.outstanding.len() > 0 {
                let msg = format!("Reader {reader_id} is going to be forgotten with outstanding items");
                warn_with_info!("{}", msg);
                reader._comment(now, msg);
            }
            reader._comment(now, format!("Reader {reader_id} is going to be forgotten"));
            for entry in reader.outstanding.iter() {
                match self.blobs.get_mut(&entry.digest) {
                    Some(sb) => {
                        let ok = sb.release_for_reader(reader_id, entry.seq);
                    }
                    None => {}
                }
            }
            self.readers.remove(reader_id);
        }
    }

    pub fn finish_for_reader(&mut self, reader_id: &ReaderID) {
        if let Some(reader) = self.readers.get_mut(reader_id) {
            reader.set_finished();
        }
    }
}

pub fn format_digest_path(digest: &str, content_type: &str, reader_id: &ReaderID, i: usize) -> String {
    let content_type_b64 = my_base64_encode_str(content_type);

    format!("!/:ipfs/{}/{}/{}/{}/", digest, content_type_b64, reader_id, i)
}
//
// struct DroppableReaderRef {
//     ssa: ServerStateAccess,
//     name: ReaderID,
// }
//
// impl Drop for DroppableReaderRef {
//     fn drop(&mut self) {
//         {
//             let mut ss = self.ssa.lock();
//             ss.blob_manager.forget_reader(&self.name);
//         }
//     }
// }
