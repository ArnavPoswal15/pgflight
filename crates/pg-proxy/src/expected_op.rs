//! Unified per-connection queue of what the backend must answer next.
//! Every frontend request pushes one entry BEFORE it is written upstream.

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExpectedOp {
    ParseComplete,                 // after Parse ('P') -> '1'
    BindComplete,                  // after Bind ('B') -> '2'
    DescribeStatement,             // 't' then 'T' or 'n'
    DescribePortal,                // 'T' or 'n'
    CloseComplete,                 // after Close ('C') -> '3'
    Execute { query_id: u64 },     // 'C' | 'I' | 's' (PortalSuspended keeps portal) | 'E'
    Sync,                          // 'Z'
    SimpleBatch { query_id: u64 }, // 0..N of 'C'/'I'/'E' until 'Z'
}

/// On backend ErrorResponse in extended mode the server discards everything
/// until Sync, so drain the queue up to (not including) the next Sync marker.
pub fn drain_to_next_sync(q: &mut std::collections::VecDeque<ExpectedOp>) {
    while let Some(op) = q.front() {
        if *op == ExpectedOp::Sync {
            break;
        }
        q.pop_front();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;

    #[test]
    fn drains_until_sync() {
        let mut q: VecDeque<_> =
            vec![ExpectedOp::ParseComplete, ExpectedOp::BindComplete, ExpectedOp::Sync].into();
        drain_to_next_sync(&mut q);
        assert_eq!(q.front(), Some(&ExpectedOp::Sync));
    }
}
