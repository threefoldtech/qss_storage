use std::ops::RangeBounds;
use std::sync::Arc;

use fjall::{self, Readable, SingleWriterTxKeyspace};

use super::{AckPersist, TxDb};
use crate::metastore::{BaseMetaTree, KeyValuePairs, MetaError, MetaTreeExt, Object};

/// A metadata tree: one keyspace of the fjall database, plus the database
/// handle needed to read from it.
///
/// The transactional keyspace handle has no iteration API of its own, so
/// reads go through a read transaction on the database. The returned
/// [`fjall::Iter`] owns its snapshot nonce and so stays valid after the read
/// transaction that produced it is dropped.
pub struct FjallTree {
    db: TxDb,
    partition: Arc<SingleWriterTxKeyspace>,
    ack_persist: AckPersist,
}

impl FjallTree {
    pub fn new(db: TxDb, partition: Arc<SingleWriterTxKeyspace>, ack_persist: AckPersist) -> Self {
        Self {
            db,
            partition,
            ack_persist,
        }
    }

    /// Forward-ordered iterator over a key range.
    fn range<R: RangeBounds<Vec<u8>>>(
        db: &TxDb,
        partition: &SingleWriterTxKeyspace,
        range: R,
    ) -> fjall::Iter {
        db.db.read_tx().range::<Vec<u8>, _>(partition, range)
    }

    /// Iterator over all keys carrying `prefix`.
    fn prefix(db: &TxDb, partition: &SingleWriterTxKeyspace, prefix: &[u8]) -> fjall::Iter {
        db.db.read_tx().prefix(partition, prefix)
    }
}

impl BaseMetaTree for FjallTree {
    /// # Durability
    ///
    /// Callers acknowledge on the strength of this returning `Ok`:
    /// `CreateBucket`, `CreateMultipartUpload`, `UploadPart`'s ETag and
    /// respcas's `SET` all come through here rather than through a
    /// transaction. What that ack promises is the tree's [`AckPersist`]
    /// class (ADR 0013):
    ///
    /// - `Contract` trees persist at the store's configured durability
    ///   before returning, because losing the write later would be silent.
    /// - `Recoverable` trees (`_MULTIPART_PARTS`, `_UPLOADS`) rely on
    ///   fjall's internal kernel-visible write: a power cut can take the
    ///   record, and the protocol answers with InvalidPart / NoSuchUpload
    ///   -- loud, retryable, leak-class at worst (over-counted blocks the
    ///   recount collects).
    ///
    /// Either way the bytes are the kernel's before the ack, so a
    /// `kill -9` takes nothing -- the campaign's crash matrix grades that
    /// half; the power-loss half is the contract table itself.
    fn insert(&self, key: &[u8], value: Vec<u8>) -> Result<(), MetaError> {
        self.partition
            .insert(key, value)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        match self.ack_persist {
            AckPersist::Contract => self.db.persist(),
            AckPersist::Recoverable => Ok(()),
        }
    }

    /// # Durability
    ///
    /// Same class split as [`insert`](Self::insert). A removal a client
    /// was told succeeded -- respcas's `DEL` -- must not come back, and
    /// respcas's trees are `Contract` class.
    fn remove(&self, key: &[u8]) -> Result<bool, MetaError> {
        // fjall's remove does not say whether the key was there, so the
        // existence is probed first; the two ops are not one transaction,
        // which is the "best-effort" in the trait contract.
        let existed = self
            .partition
            .contains_key(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        self.partition
            .remove(key)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))?;
        match self.ack_persist {
            AckPersist::Contract => self.db.persist()?,
            AckPersist::Recoverable => {}
        }
        Ok(existed)
    }

    fn contains_key(&self, key: &[u8]) -> Result<bool, MetaError> {
        self.partition
            .contains_key(key)
            .map_err(|_| MetaError::KeyNotFound)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        match self.partition.get(key) {
            Ok(v) => Ok(v.map(|v| v.to_vec())),
            Err(e) => Err(MetaError::OtherDBError(e.to_string())),
        }
    }

    // Upstream marks `len` `#[cfg(test)]`; it is needed at runtime here (see
    // EXTENSIONS.md).
    fn len(&self) -> Result<usize, MetaError> {
        self.db
            .db
            .read_tx()
            .len(&*self.partition)
            .map_err(|e| MetaError::OtherDBError(e.to_string()))
    }
}

impl MetaTreeExt for FjallTree {
    fn iter_all(&self) -> KeyValuePairs {
        self.iter_kv(None)
    }

    fn iter_kv(&self, start_after: Option<Vec<u8>>) -> KeyValuePairs {
        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_after;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => {
                    let mut next = k.clone();
                    next.push(0);
                    next..
                }
                None => Vec::new()..,
            };

            Self::range(&db, &partition, range)
                .next()
                .map(|guard| advance(guard, &mut last_key))
        }))
    }

    /// The mirror of [`iter_kv`](Self::iter_kv): no cursor starts at the
    /// LARGEST key, a cursor resumes strictly below itself.
    ///
    /// The no-cursor range has to be unbounded on both ends, not `..empty`:
    /// the empty key is the smallest key there is, so a range below it is
    /// always empty and `RSCAN 0` -- whose `0` the command layer reads as "no
    /// cursor", exactly as `SCAN 0` does -- answered nothing whatever the
    /// tree held.
    fn iter_kv_backward(&self, start_key: Option<Vec<u8>>) -> KeyValuePairs {
        use std::ops::Bound;

        let partition = self.partition.clone();
        let db = self.db.clone();
        let mut last_key = start_key;

        Box::new(std::iter::from_fn(move || {
            let range = match &last_key {
                Some(k) => (Bound::Unbounded, Bound::Excluded(k.clone())),
                None => (Bound::Unbounded, Bound::Unbounded),
            };

            Self::range(&db, &partition, range)
                .next_back()
                .map(|guard| advance(guard, &mut last_key))
        }))
    }

    // rules:
    // 1. continuation_token and start_after exists: use the one with the highest lexicographical order
    //    -> call it: ctsa
    // 2. if prefix exists
    //    -> ctsa > the prefix && doesn't have prefix: return zero results
    //    -> ctsa < prefix: ignore it
    //    -> ctsa has the prefix: use it as start_after
    //          In kv store like fjall & Sled: we process it in the Rust code
    fn range_filter<'a>(
        &'a self,
        start_after: Option<String>,
        prefix: Option<String>,
        continuation_token: Option<String>,
    ) -> Box<dyn Iterator<Item = (String, Object)> + 'a> {
        let mut ctsa = match (continuation_token, start_after) {
            (Some(token), Some(start)) => Some(std::cmp::max(token, start)),
            (Some(token), None) => Some(token),
            (None, start) => start,
        };

        let db = &self.db;
        let partition = &self.partition;

        let base_iter: Box<dyn Iterator<Item = fjall::Guard>> =
            match (prefix.as_ref(), ctsa.as_ref()) {
                (Some(prefix), Some(ctsa)) if (ctsa > prefix && !ctsa.starts_with(prefix)) => {
                    //Return empty iterator if ctsa is after prefix
                    Box::new(std::iter::empty())
                }
                (Some(prefix), Some(ctsa_local)) if ctsa_local < prefix => {
                    // If ctsa is before prefix, ignore ctsa
                    ctsa = None;
                    Box::new(Self::prefix(db, partition, prefix.as_bytes()))
                }
                (Some(prefix), _) => Box::new(Self::prefix(db, partition, prefix.as_bytes())),
                (None, Some(ctsa)) => {
                    let mut next_key = ctsa.as_bytes().to_vec();
                    next_key.push(0);
                    Box::new(Self::range(db, partition, next_key..))
                }
                (None, None) => Box::new(Self::range(db, partition, ..)),
            };

        let pairs = base_iter.filter_map(|g| g.into_inner().ok());

        let skip_filtered = if let (Some(_), Some(ctsa)) = (&prefix, ctsa) {
            let ctsa_bytes = ctsa.into_bytes();
            Box::new(pairs.skip_while(move |(raw_key, _)| &**raw_key <= ctsa_bytes.as_slice()))
                as Box<dyn Iterator<Item = _>>
        } else {
            Box::new(pairs)
        };

        // Upstream used `String::from_utf8_unchecked` on the raw key and
        // `unwrap()`ed the value decode. Both come straight off disk, so a
        // corrupt or truncated record turned into undefined behaviour or a
        // panic instead of a bad result. `range_filter` yields an infallible
        // item type, so a key that is not valid UTF-8 and a value that fails to
        // decode are both skipped and logged -- the same treatment the iterator
        // above already gives to keys the backend fails to read
        // (`filter_map(|g| g.into_inner().ok())`).
        Box::new(skip_filtered.filter_map(|(raw_key, raw_value)| {
            let key = match String::from_utf8(raw_key.to_vec()) {
                Ok(key) => key,
                Err(e) => {
                    tracing::error!("Skipping key that is not valid UTF-8: {}", e);
                    return None;
                }
            };
            let obj = match Object::try_from(&*raw_value) {
                Ok(obj) => obj,
                Err(e) => {
                    tracing::error!("Skipping key {} with an undecodable object: {}", key, e);
                    return None;
                }
            };
            Some((key, obj))
        }))
    }
}

/// Decodes one guard into an owned key-value pair and records the key as the
/// cursor for the next step of an `iter_kv`/`iter_kv_backward` walk.
fn advance(
    guard: fjall::Guard,
    cursor: &mut Option<Vec<u8>>,
) -> Result<(Vec<u8>, Vec<u8>), MetaError> {
    match guard.into_inner() {
        Ok((k, v)) => {
            *cursor = Some(k.to_vec());
            Ok((k.to_vec(), v.to_vec()))
        }
        Err(e) => {
            tracing::error!("Error reading key: {}", e);
            Err(MetaError::OtherDBError(e.to_string()))
        }
    }
}
