//! What a namespace is, who may change it, and who may read it.
//!
//! NSSET is the only way a namespace's behaviour is configured and NSINFO is
//! the only way it is read back, so the pair is the whole surface: a property
//! that does not survive the round trip is a property an operator cannot
//! trust. The enforcement half is here too -- a flag NSINFO reports and
//! nothing acts on is worse than no flag.

mod common;

#[path = "namespace_test/helpers.rs"]
mod helpers;

#[path = "namespace_test/access.rs"]
mod access;
#[path = "namespace_test/limits.rs"]
mod limits;
#[path = "namespace_test/properties.rs"]
mod properties;
#[path = "namespace_test/usage.rs"]
mod usage;
