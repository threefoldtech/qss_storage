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
}

impl Default for SharedMetrics {
    fn default() -> Self {
        Self(Arc::new(NoOpMetrics))
    }
}
