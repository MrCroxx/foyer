//  Copyright 2024 Foyer Project Authors
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

use std::collections::VecDeque;

use crate::{IoBuffer, IoBytes};

pub enum Buffer {
    IoBuffer(IoBuffer),
    IoBytes(IoBytes),
}

impl From<IoBuffer> for Buffer {
    fn from(value: IoBuffer) -> Self {
        Self::IoBuffer(value)
    }
}

impl From<IoBytes> for Buffer {
    fn from(value: IoBytes) -> Self {
        Self::IoBytes(value)
    }
}

pub struct IoBufferPool {
    capacity: usize,
    buffer_size: usize,
    queue: VecDeque<Buffer>,
}

impl IoBufferPool {
    pub fn new(buffer_size: usize, capacity: usize) -> Self {
        Self {
            capacity,
            buffer_size,
            queue: VecDeque::with_capacity(capacity),
        }
    }

    pub fn acquire(&mut self) -> IoBuffer {
        let create = || IoBuffer::new(self.buffer_size);
        let res = match self.queue.pop_front() {
            Some(Buffer::IoBuffer(buffer)) => buffer,
            Some(Buffer::IoBytes(bytes)) => bytes.into_io_buffer().unwrap_or_else(create),
            None => create(),
        };
        assert_eq!(res.len(), self.buffer_size);
        res
    }

    pub fn release(&mut self, buffer: impl Into<Buffer>) {
        if self.queue.len() < self.capacity {
            self.queue.push_back(buffer.into());
        }
    }
}
