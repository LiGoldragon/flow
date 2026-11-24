//! The public request identifier's fixed encoded width.

use crate::{FixedWidth, RequestId};

impl FixedWidth for RequestId {
    const WIDTH: i64 = 4;
}
