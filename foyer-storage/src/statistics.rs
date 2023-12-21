//  Copyright 2023 MrCroxx
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//! The differences between [`crate::metrics::Metrics`] and [`Statistics`] is that
//! [`crate::metrics::Metrics`] is used to expose the internal system status to
//! external systems, without high requirements for accuracy and data freshness,
//! while [`Statistics`] is used as a reference for internal system components to
//! sense the system status and adjust parameters, with higher requirements for
//! accuracy and data freshness.

use std::sync::atomic::AtomicUsize;

#[derive(Debug, Default)]
pub struct Statistics {
    /// Written bytes can be used to implement rate limit policy.
    /// It is more accuracy than [`crate::metrics::Metrics::op_bytes_flush`]
    /// because it updates per entry without batching.
    pub written_bytes: AtomicUsize,
}
