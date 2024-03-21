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

use bytes::{Buf, BufMut};

pub trait BufExt: Buf {
    cfg_match! {
        cfg(target_pointer_width = "16") => {
            fn get_usize(&mut self) -> usize {
                self.get_u16() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i16() as isize
            }
        }
        cfg(target_pointer_width = "32") => {
            fn get_usize(&mut self) -> usize {
                self.get_u32() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i32() as isize
            }
        }
        cfg(target_pointer_width = "64") => {
            fn get_usize(&mut self) -> usize {
                self.get_u64() as usize
            }

            fn get_isize(&mut self) -> isize {
                self.get_i64() as isize
            }
        }
    }
}

impl<T: Buf> BufExt for T {}

pub trait BufMutExt: BufMut {
    cfg_match! {
        cfg(target_pointer_width = "16") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u16(v as u16);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i16(v as i16);
            }
        }
        cfg(target_pointer_width = "32") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u32(v as u32);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i32(v as i32);
            }
        }
        cfg(target_pointer_width = "64") => {
            fn put_usize(&mut self, v: usize) {
                self.put_u64(v as u64);
            }

            fn put_isize(&mut self, v: isize) {
                self.put_i64(v as i64);
            }
        }
    }
}

impl<T: BufMut> BufMutExt for T {}
