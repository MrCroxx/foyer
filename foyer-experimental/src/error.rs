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

use std::backtrace::Backtrace;

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct Error(Box<ErrorInner>);

#[derive(thiserror::Error, Debug)]
#[error("{source:?}")]
struct ErrorInner {
    #[from]
    source: ErrorKind,
    backtrace: Backtrace,
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorKind {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        value.into()
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        value.into()
    }
}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        value.into()
    }
}

pub type Result<T> = core::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_size() {
        assert_eq!(std::mem::size_of::<Error>(), std::mem::size_of::<usize>());
    }
}
