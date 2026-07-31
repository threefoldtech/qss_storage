use std::sync::Arc;

/// Shared metrics collector interface
///
/// This is a trait object that allows applications to plug in their own
/// metrics implementations (Prometheus, StatsD, etc.)
pub trait MetricsCollector: Send + Sync {
    fn block_pending(&self);
    fn block_written(&self);
    fn block_write_error(&self);
    fn block_ignored(&self);
    fn blocks_dropped(&self, amount: u64);
    fn bytes_sent(&self, amount: usize);
    fn bytes_received(&self, amount: usize);

    /// A blocking block-disk closure (write- or delete-side, ADR 0006) was
    /// submitted to the blocking pool. Paired with
    /// [`block_disk_op_finished`](Self::block_disk_op_finished); the gap
    /// between the two is the in-flight gauge, and its growth against the
    /// completion rate is the queue-depth signal the ADR requires.
    ///
    /// Default no-op so existing collectors keep compiling.
    fn block_disk_op_started(&self) {}

    /// The blocking closure completed (success or failure alike).
    fn block_disk_op_finished(&self) {}
}

/// No-op metrics collector (default)
#[derive(Debug, Clone, Default)]
pub struct NoOpMetrics;

impl MetricsCollector for NoOpMetrics {
    fn block_pending(&self) {}
    fn block_written(&self) {}
    fn block_write_error(&self) {}
    fn block_ignored(&self) {}
    fn blocks_dropped(&self, _amount: u64) {}
    fn bytes_sent(&self, _amount: usize) {}
    fn bytes_received(&self, _amount: usize) {}
}

/// Shared reference to metrics collector
#[derive(Clone)]
pub struct SharedMetrics(Arc<dyn MetricsCollector>);

impl SharedMetrics {
    pub fn new(collector: Arc<dyn MetricsCollector>) -> Self {
        Self(collector)
    }

    pub fn block_pending(&self) {
        self.0.block_pending();
    }

    pub fn block_written(&self) {
        self.0.block_written();
    }

    pub fn block_write_error(&self) {
        self.0.block_write_error();
    }

    pub fn block_ignored(&self) {
        self.0.block_ignored();
    }

    pub fn blocks_dropped(&self, amount: u64) {
        self.0.blocks_dropped(amount);
    }

    pub fn bytes_sent(&self, amount: usize) {
        self.0.bytes_sent(amount);
    }

    pub fn bytes_received(&self, amount: usize) {
        self.0.bytes_received(amount);
    }

    pub fn block_disk_op_started(&self) {
        self.0.block_disk_op_started();
    }

    pub fn block_disk_op_finished(&self) {
        self.0.block_disk_op_finished();
    }
}

impl Default for SharedMetrics {
    fn default() -> Self {
        Self(Arc::new(NoOpMetrics))
    }
}
