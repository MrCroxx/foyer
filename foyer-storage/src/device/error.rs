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

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct DeviceError(Box<DeviceErrorInner>);

#[derive(thiserror::Error, Debug)]
#[error("{source}")]
struct DeviceErrorInner {
    source: DeviceErrorKind,
    // https://github.com/dtolnay/thiserror/issues/204
    // backtrace: Backtrace,
}

#[derive(thiserror::Error, Debug)]
pub enum DeviceErrorKind {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("nix error: {0}")]
    Nix(#[from] nix::errno::Errno),
    #[error("other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync + 'static>),
}

impl From<std::io::Error> for DeviceError {
    fn from(value: std::io::Error) -> Self {
        value.into()
    }
}

impl From<nix::errno::Errno> for DeviceError {
    fn from(value: nix::errno::Errno) -> Self {
        value.into()
    }
}

impl From<String> for DeviceError {
    fn from(value: String) -> Self {
        value.into()
    }
}

pub type DeviceResult<T> = std::result::Result<T, DeviceError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_size() {
        assert_eq!(std::mem::size_of::<DeviceError>(), std::mem::size_of::<usize>());
    }
}
