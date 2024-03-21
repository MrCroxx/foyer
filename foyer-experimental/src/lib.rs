//  Copyright 2024 Foyer Project Authors.
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

#![feature(cfg_match)]
#![feature(lint_reasons)]
#![feature(error_generic_member_access)]
#![feature(lazy_cell)]

pub mod buf;
pub mod error;
pub mod metrics;
pub mod notify;
pub mod wal;

#[cfg(not(madsim))]
#[tracing::instrument(level = "trace", skip(f))]
async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

#[cfg(madsim)]
#[tracing::instrument(level = "trace", skip(f))]
async fn asyncify<F, T>(f: F) -> T
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    f()
}
