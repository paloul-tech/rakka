//! Placeholder task projection boundary.
//!
//! A2A reads, streaming, and push delivery should eventually come from durable
//! task projections. Phase 0 deliberately has no projection-backed task data.

use a2a::{ListTasksResponse, Task};

/// Empty task projection used while durable A2A handling is not implemented.
#[derive(Debug, Clone, Default)]
pub struct Phase0TaskProjection;

impl Phase0TaskProjection {
    /// Returns an empty A2A list response.
    #[must_use]
    pub fn empty_list(page_size: Option<i32>) -> ListTasksResponse {
        ListTasksResponse {
            tasks: Vec::<Task>::new(),
            next_page_token: String::new(),
            page_size: page_size.unwrap_or(0),
            total_size: 0,
        }
    }
}
