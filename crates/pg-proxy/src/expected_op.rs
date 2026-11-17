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

/// Map a frontend frame to expected backend op(s). Call before writing upstream.
pub fn push_for_frontend(
    q: &mut std::collections::VecDeque<ExpectedOp>,
    tag: u8,
    body: &[u8],
    next_query_id: &mut u64,
) {
    match tag {
        b'P' => q.push_back(ExpectedOp::ParseComplete),
        b'B' => q.push_back(ExpectedOp::BindComplete),
        b'D' => {
            if body.first() == Some(&b'S') {
                q.push_back(ExpectedOp::DescribeStatement);
            } else {
                q.push_back(ExpectedOp::DescribePortal);
            }
        }
        b'E' => {
            let qid = *next_query_id;
            *next_query_id += 1;
            q.push_back(ExpectedOp::Execute { query_id: qid });
        }
        b'C' => q.push_back(ExpectedOp::CloseComplete),
        b'S' => q.push_back(ExpectedOp::Sync),
        b'Q' => {
            let qid = *next_query_id;
            *next_query_id += 1;
            q.push_back(ExpectedOp::SimpleBatch { query_id: qid });
        }
        _ => {}
    }
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
    fn push_parse_and_sync() {
        let mut q = VecDeque::new();
        let mut qid = 1;
        push_for_frontend(&mut q, b'P', &[], &mut qid);
        push_for_frontend(&mut q, b'S', &[], &mut qid);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn drains_until_sync() {
        let mut q: VecDeque<_> = vec![
            ExpectedOp::ParseComplete,
            ExpectedOp::BindComplete,
            ExpectedOp::Sync,
        ]
        .into();
        drain_to_next_sync(&mut q);
        assert_eq!(q.front(), Some(&ExpectedOp::Sync));
    }
}
