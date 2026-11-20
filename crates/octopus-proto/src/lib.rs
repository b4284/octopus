//! Octopus wire protocol (see `proto/octopus.proto`).

pub mod v1 {
    include!(concat!(env!("OUT_DIR"), "/octopus.v1.rs"));
}

pub use prost::Message;

/// Topic layout under a namespace.
#[derive(Clone, Debug)]
pub struct Topics {
    pub ns: String,
}

/// Shared-subscription group name used by every dispatcher.
pub const DISPATCHER_SHARE: &str = "octopusd";

impl Topics {
    pub fn new(ns: impl Into<String>) -> Self {
        Self { ns: ns.into() }
    }
    pub fn publish(&self, group: &str) -> String {
        format!("{}/pub/{group}", self.ns)
    }
    pub fn publish_shared_filter(&self) -> String {
        format!("$share/{DISPATCHER_SHARE}/{}/pub/+", self.ns)
    }
    pub fn any(&self) -> String {
        format!("{}/any", self.ns)
    }
    pub fn any_shared_filter(&self) -> String {
        format!("$share/{DISPATCHER_SHARE}/{}/any", self.ns)
    }
    pub fn inbox(&self, id: &str) -> String {
        format!("{}/inbox/{id}", self.ns)
    }
    pub fn member(&self, member: &str) -> String {
        format!("{}/member/{member}", self.ns)
    }
    pub fn members_filter(&self) -> String {
        format!("{}/member/+", self.ns)
    }
    pub fn owner(&self, group: &str) -> String {
        format!("{}/owner/{group}", self.ns)
    }
    pub fn owners_filter(&self) -> String {
        format!("{}/owner/+", self.ns)
    }
}

/// Ids and group names become topic levels: no separators or wildcards, not empty.
pub fn valid_name(s: &str) -> bool {
    !s.is_empty() && s.len() <= 200 && !s.contains(['/', '+', '#']) && !s.starts_with('$')
}
