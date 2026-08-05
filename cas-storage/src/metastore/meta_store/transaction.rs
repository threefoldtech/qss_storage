use super::{
    Block, BlockDecrement, DEFAULT_BLOCK_TREE, DEFAULT_BUCKET_TREE, DEFAULT_USAGE_TREE,
    MULTIPART_PARTS_TREE, MetaError, Transaction, TransactionBackend, UPLOADS_TREE, decode_usage,
};
use crate::metastore::{BlockId, Object, UploadRecord};

impl Transaction {
    /// Creates a new Transaction with the given backend.
    ///
    /// # Arguments
    /// * `backend` - The transaction backend implementation
    ///
    /// # Returns
    /// A new Transaction instance
    pub(crate) fn new(backend: Box<dyn TransactionBackend>) -> Self {
        Self { backend }
    }

    /// Commits the transaction, making all changes permanent.
    ///
    /// # Returns
    /// Success or an error if the commit fails
    pub fn commit(mut self) -> Result<(), MetaError> {
        self.backend.commit()
    }

    /// Rolls back the transaction, discarding all changes.
    ///
    /// This method is called when the transaction should be aborted.
    pub fn rollback(mut self) {
        // Call the backend's rollback method for cleanup
        self.backend.rollback();
    }

    /// The dedup half of the ADR 0006 write protocol: if a record for
    /// `block_hash` exists, bump its reference count by one INSIDE this
    /// transaction and return the updated block; otherwise return `None`
    /// and change nothing.
    ///
    /// The existence check and the rc mutation are one transactional
    /// read-modify-write (hard rule 2) -- never split this into a pre-tx
    /// read with a blind write. Every dedup hit bumps: the old
    /// `key_has_block` skip undercounted references and is gone (ADR 0006).
    ///
    /// A record flagged degraded reports as absent (`None`): its file is
    /// gone, so deduplicating against it would commit another damaged
    /// object (ADR 0005). The caller falls through to the insert path,
    /// which heals the record instead.
    ///
    /// The caller must hold the block's stripe (hard rule 1).
    pub fn bump_block_rc(&mut self, block_hash: BlockId) -> Result<Option<Block>, MetaError> {
        match self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        {
            None => Ok(None),
            Some(block_data) => {
                let mut block = Block::try_from(&*block_data as &[u8])?;
                if block.is_degraded() {
                    tracing::debug!(
                        block_hash = %block_hash.to_hex(),
                        rc = block.rc(),
                        "Block record is degraded: absent for dedup, heal on insert"
                    );
                    return Ok(None);
                }
                let old_rc = block.rc();
                block.increment_refcount();
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    old_rc = old_rc,
                    new_rc = block.rc(),
                    "Block exists: incrementing refcount"
                );
                self.backend
                    .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;
                Ok(Some(block))
            }
        }
    }

    /// The DELETE-side object step (ADR 0006): reads AND removes the object
    /// record for `key` inside this transaction, so read+remove commit as
    /// one atomic pair -- that is what defeats a concurrent double-DELETE
    /// of one key double-decrementing block refcounts (verified defect 5's
    /// sibling).
    ///
    /// Returns the removed object, or `None` (and no change) if the key was
    /// absent -- DELETE is idempotent.
    pub fn take_object(&mut self, bucket: &str, key: &[u8]) -> Result<Option<Object>, MetaError> {
        let Some(raw) = self.backend.get(bucket, key)? else {
            return Ok(None);
        };
        let obj = Object::try_from(&*raw)?;
        self.backend.remove(bucket, key)?;
        Ok(Some(obj))
    }

    /// The overwrite-side object step (ADR 0008): reads the object record
    /// for `key` AND writes `raw_obj` over it inside this transaction,
    /// returning what it displaced (`None` if the key was free).
    ///
    /// [`take_object`](Self::take_object)'s sibling, and for the same
    /// reason: the read and the write must commit as one atomic pair. The
    /// returned record is what THIS writer displaced, so it is this
    /// writer's to release -- and no other writer's. Two concurrent
    /// overwrites of one key serialize under the single-writer transaction:
    /// the first displaces the original, the second displaces the first's
    /// record, and every record is released exactly once by exactly the
    /// writer that replaced it.
    ///
    /// Splitting the read from the write is the bug this shape exists to
    /// prevent. Both writers would read the same displaced record and both
    /// would release its blocks: a double decrement per occurrence, which
    /// is the loss direction.
    ///
    /// # The caller releases AFTER the commit, never before
    ///
    /// The new record is durable before the old references are dropped. A
    /// crash in between leaves the old blocks over-counted -- leakage, INFO,
    /// collected by the next recount. The reverse order would drop
    /// references while the old record is still the visible one, so a reader
    /// resolving that record races an unlink with nothing holding the block:
    /// loss. See `release_blocks` for the same argument on the delete side.
    ///
    /// A displaced record that will not decode fails the whole call and
    /// nothing is written. Writing over a record whose block list could not
    /// be read would strand every reference it names, with no holder left
    /// that can ever name them again.
    pub fn replace_object(
        &mut self,
        bucket: &str,
        key: &[u8],
        raw_obj: Vec<u8>,
    ) -> Result<Option<Object>, MetaError> {
        let displaced = match self.backend.get(bucket, key)? {
            Some(raw) => Some(Object::try_from(&*raw)?),
            None => None,
        };
        self.backend.insert(bucket, key, raw_obj)?;
        Ok(displaced)
    }

    /// Reads the raw bucket record for `bucket_name` inside this transaction.
    ///
    /// Raw bytes rather than a decoded [`crate::metastore::BucketMeta`]: the record under a
    /// bucket name is not always one -- respcas keeps its own namespace
    /// metadata there -- and the caller that asked whether the name is taken
    /// does not care which.
    pub fn get_bucket_record(&mut self, bucket_name: &str) -> Result<Option<Vec<u8>>, MetaError> {
        self.backend
            .get(DEFAULT_BUCKET_TREE, bucket_name.as_bytes())
    }

    /// Writes the raw bucket record for `bucket_name` inside this
    /// transaction, over whatever is there.
    ///
    /// Paired with [`get_bucket_record`](Self::get_bucket_record) it is the
    /// insert-if-absent [`super::MetaStore::insert_bucket_if_absent`] is built from:
    /// the two steps commit together, so a name cannot be claimed twice.
    pub fn put_bucket_record(
        &mut self,
        bucket_name: &str,
        raw_bucket: Vec<u8>,
    ) -> Result<(), MetaError> {
        self.backend
            .insert(DEFAULT_BUCKET_TREE, bucket_name.as_bytes(), raw_bucket)
    }

    /// Moves `bucket`'s usage counter by `delta` logical bytes inside this
    /// transaction, and returns what it now reads.
    ///
    /// Called from the same transaction as the record mutation it accounts
    /// for -- the counter and the records live in one database, so they
    /// commit or roll back together and no crash can leave one without the
    /// other. The deltas are: a stored record adds its size, a removed record
    /// subtracts it, an overwrite applies the difference (which is what
    /// [`replace_object`](Self::replace_object) returning the displaced
    /// record is for), and a clone adds the full size in the DESTINATION
    /// bucket -- a clone is a store on this ledger, however few bytes it
    /// moves (ADR 0014).
    ///
    /// A decrement below zero is clamped and logged rather than wrapped: the
    /// counter is bookkeeping, and a wrong number that is loudly wrong beats
    /// eighteen quintillion bytes of quota.
    pub fn add_bucket_usage(&mut self, bucket: &str, delta: i64) -> Result<u64, MetaError> {
        let current = match self.backend.get(DEFAULT_USAGE_TREE, bucket.as_bytes())? {
            Some(raw) => decode_usage(bucket, &raw)?,
            None => 0,
        };

        let updated = if delta >= 0 {
            current.saturating_add(delta.unsigned_abs())
        } else {
            let down = delta.unsigned_abs();
            if down > current {
                tracing::warn!(
                    bucket = %bucket,
                    usage = current,
                    subtracted = down,
                    "The usage counter would go below zero: clamping to 0"
                );
                0
            } else {
                current - down
            }
        };

        self.backend.insert(
            DEFAULT_USAGE_TREE,
            bucket.as_bytes(),
            updated.to_le_bytes().to_vec(),
        )?;
        Ok(updated)
    }

    /// Sets `bucket`'s usage counter to zero inside this transaction: the
    /// counter a bucket starts life with, written with the record that claims
    /// its name.
    pub fn reset_bucket_usage(&mut self, bucket: &str) -> Result<(), MetaError> {
        self.backend.insert(
            DEFAULT_USAGE_TREE,
            bucket.as_bytes(),
            0u64.to_le_bytes().to_vec(),
        )
    }

    /// The multipart claim (ADR 0003): reads AND removes the upload record
    /// at `key` in [`UPLOADS_TREE`] inside this transaction.
    ///
    /// This is THE linearization point between complete and abort. Neither
    /// operation holds a lock -- none exists -- so both begin here, and the
    /// atomic read+remove is what makes exactly one of them the winner: the
    /// loser gets `None` and answers `NoSuchUpload`. Double-complete,
    /// double-abort, complete-versus-abort and the GC's own abort all
    /// collapse into this one rule, so nothing downstream of a won claim
    /// needs to re-check that the upload is still live.
    ///
    /// `None` (and no change) if the record was absent -- claiming an upload
    /// that is already gone changes nothing.
    ///
    /// Same shape as [`take_object`](Self::take_object), for the same
    /// reason: splitting the read from the remove would let two callers both
    /// read the record and both proceed.
    pub fn take_upload(&mut self, key: &[u8]) -> Result<Option<UploadRecord>, MetaError> {
        let Some(raw) = self.backend.get(UPLOADS_TREE, key)? else {
            return Ok(None);
        };
        let record = UploadRecord::try_from(&*raw)?;
        self.backend.remove(UPLOADS_TREE, key)?;
        Ok(Some(record))
    }

    /// The per-part claim (ADR 0003 amendment): reads AND removes the part
    /// record at `key` in [`MULTIPART_PARTS_TREE`] inside this transaction.
    ///
    /// Removing the record IS the claim on its blocks. Complete inherits a
    /// part's references into the object it mints; abort and the GC release
    /// them. Whoever takes the record decides which happened, and the loser
    /// of the take is told the record was already gone -- so no two callers
    /// can both act on one part's blocks, which is the difference between
    /// leakage and loss.
    ///
    /// `_UPLOADS` and `_MULTIPART_PARTS` live in the same shared database, so
    /// one transaction spans both: that is what lets complete take the upload
    /// record and every part it names as a single atomic step
    /// (`claim_upload_with_parts`).
    ///
    /// Raw bytes rather than a decoded record, unlike
    /// [`take_upload`](Self::take_upload): `MultiPart` is a `cas`-layer type
    /// and this layer names nothing from above it. The caller decodes -- and
    /// a caller whose decode fails must roll back, because an undecodable
    /// record names no blocks and removing it would strand them.
    ///
    /// `None` (and no change) if the record was absent.
    pub fn take_part(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, MetaError> {
        let Some(raw) = self.backend.get(MULTIPART_PARTS_TREE, key)? else {
            return Ok(None);
        };
        self.backend.remove(MULTIPART_PARTS_TREE, key)?;
        Ok(Some(raw))
    }

    /// The DELETE-side block step (ADR 0006): re-checks the record and
    /// applies one reference decrement inside this transaction.
    ///
    /// The re-check matters: between the object removal and this call,
    /// other writers may have bumped or even removed-and-recreated the
    /// record, so the caller's earlier knowledge is stale. Under the stripe
    /// this read-modify-write is race-free (hard rules 1 and 2).
    ///
    /// On [`BlockDecrement::Removed`] the caller must commit FIRST and then
    /// unlink the file at the returned block's depth, all inside the same
    /// stripe hold and the same blocking closure (hard rule 4) -- a
    /// cancellable await between decrement and unlink is how a detached
    /// unlink once deleted a freshly rewritten block.
    ///
    /// A degraded record (ADR 0005) needs no special case: it accounts for
    /// real holders, so it decrements like any other, and the unlink that
    /// follows its last reference tolerates ENOENT -- having no file is
    /// exactly what degraded means.
    pub fn decrement_block_rc(&mut self, block_hash: BlockId) -> Result<BlockDecrement, MetaError> {
        let Some(raw) = self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        else {
            return Ok(BlockDecrement::Missing);
        };
        let mut block = Block::try_from(&*raw)?;

        if block.rc() == 1 {
            tracing::debug!(
                block_hash = %block_hash.to_hex(),
                "Block rc==1: removing record; caller unlinks the file"
            );
            self.backend
                .remove(DEFAULT_BLOCK_TREE, block_hash.as_slice())?;
            return Ok(BlockDecrement::Removed(block));
        }

        let old_rc = block.rc();
        block.decrement_refcount();
        tracing::debug!(
            block_hash = %block_hash.to_hex(),
            old_rc = old_rc,
            new_rc = block.rc(),
            "Block rc>1: decrementing refcount"
        );
        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;
        Ok(BlockDecrement::Decremented(block))
    }

    /// The new-block half of the ADR 0006 write protocol: write the record
    /// for `block_hash`, whose file is now durable at `depth`.
    ///
    /// Callable only after the block's file is durable at its final path
    /// (hard rule 5) and only while holding the block's stripe (hard
    /// rule 1). Under the stripe there are exactly two states to meet,
    /// because only stripe holders write block records and
    /// [`bump_block_rc`](Self::bump_block_rc) has just reported this one
    /// absent-or-degraded:
    ///
    /// - no record: insert a fresh one at rc = 1;
    /// - a degraded record (ADR 0005): the file we just wrote is the heal.
    ///   Clear the flag, take the depth the file actually landed at -- the
    ///   heal's placement need not match the depth the dead record named,
    ///   and the record must follow the file -- and add our own reference
    ///   to the holders it was keeping accounted for.
    pub fn insert_new_block(
        &mut self,
        block_hash: BlockId,
        data_len: usize,
        depth: u8,
    ) -> Result<Block, MetaError> {
        let present = match self
            .backend
            .get(DEFAULT_BLOCK_TREE, block_hash.as_slice())?
        {
            Some(raw) => Some(Block::try_from(&*raw)?),
            None => None,
        };

        let block = match present {
            None => {
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    data_len = data_len,
                    depth = depth,
                    "Creating new block with rc=1"
                );
                Block::new(data_len, depth)
            }
            Some(mut present) => {
                debug_assert!(
                    present.is_degraded(),
                    "a live record under our own stripe hold: the bump would have taken it"
                );
                debug_assert_eq!(
                    present.size(),
                    data_len,
                    "same block id, same content, same size"
                );
                tracing::debug!(
                    block_hash = %block_hash.to_hex(),
                    rc = present.rc(),
                    old_depth = present.depth(),
                    depth = depth,
                    "Healing degraded block: clearing the flag and adding our reference"
                );
                // Only the degraded bit moves; the reserved bits are not
                // this build's to interpret, so they are carried over.
                present.set_degraded(false);
                Block::from_parts(data_len, depth, present.rc() + 1, present.flags())
            }
        };

        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())?;

        Ok(block)
    }

    /// Writes `block`'s record for `block_hash` verbatim, replacing whatever
    /// is there.
    ///
    /// The raw record write fsck's repair actions and the crash fixtures
    /// need: rc, depth and flags are whatever the caller states, so none of
    /// the protocol's accounting rules apply. Daemon paths use the striped
    /// read-modify-writes above instead.
    ///
    /// Callers are ADR 0005's repair actions, which run offline under the
    /// store's exclusive open, and the crash fixtures. A daemon path
    /// reaching for this would be writing an rc it did not derive under the
    /// stripe.
    pub(crate) fn put_block_record(
        &mut self,
        block_hash: BlockId,
        block: &Block,
    ) -> Result<(), MetaError> {
        self.backend
            .insert(DEFAULT_BLOCK_TREE, block_hash.as_slice(), block.to_vec())
    }

    /// Removes the record for `block_hash`; an absent record is not an
    /// error.
    ///
    /// The counterpart of [`put_block_record`](Self::put_block_record) for
    /// the one repair that must erase rather than rewrite: a recount that
    /// comes back zero. The protocol has no rc=0 state -- the daemon's
    /// decrement removes the record at its last reference -- so fsck's
    /// set-rc removes the record here and unlinks the file, exactly as
    /// [`decrement_block_rc`](Self::decrement_block_rc) would have.
    pub(crate) fn remove_block_record(&mut self, block_hash: BlockId) -> Result<(), MetaError> {
        self.backend
            .remove(DEFAULT_BLOCK_TREE, block_hash.as_slice())
    }
}
