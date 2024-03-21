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

use crate::{buffer::BufferError, device::error::DeviceError};

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct Error(Box<ErrorInner>);

#[derive(thiserror::Error, Debug)]
#[error("{source}")]
struct ErrorInner {
    source: ErrorKind,
    // https://github.com/dtolnay/thiserror/issues/204
    // backtrace: Backtrace,
}

#[derive(thiserror::Error, Debug)]
pub enum ErrorKind {
    #[error("device error: {0}")]
    Device(#[from] DeviceError),
    #[error("buffer error: {0}")]
    Buffer(anyhow::Error),
    #[error("other error: {0}")]
    Other(#[from] anyhow::Error),
}

impl From<ErrorKind> for Error {
    fn from(value: ErrorKind) -> Self {
        value.into()
    }
}

impl From<DeviceError> for Error {
    fn from(value: DeviceError) -> Self {
        value.into()
    }
}

impl<R> From<BufferError<R>> for Error
where
    R: Send + Sync + 'static + std::fmt::Debug,
{
    fn from(value: BufferError<R>) -> Self {
        match value {
            BufferError::NeedRotate(_) => panic!("BufferError::NeedRotate should not be raised!"),
            BufferError::Device(e) => From::from(e),
            BufferError::Other(e) => From::from(e),
        }
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
