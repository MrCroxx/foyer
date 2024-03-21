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

const TEXT: &[u8] = include_bytes!("../etc/sample.txt");

pub fn text(offset: usize, len: usize) -> Vec<u8> {
    let mut res = Vec::with_capacity(len);
    let mut cursor = offset % TEXT.len();
    let mut remain = len;
    while remain > 0 {
        let bytes = std::cmp::min(remain, TEXT.len() - cursor);
        res.extend(&TEXT[cursor..cursor + bytes]);
        cursor = (cursor + bytes) % TEXT.len();
        remain -= bytes;
    }
    res
}
