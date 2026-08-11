#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostFatal {
    pub reason: &'static str,
}

impl HostFatal {
    pub const fn new(reason: &'static str) -> Self {
        Self { reason }
    }
}
