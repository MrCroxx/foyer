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

use std::ops::{BitAnd, BitOr};

use bitmaps::Bitmap;

#[derive(Debug)]
pub struct Judges {
    /// 1: admit
    /// 0: reject
    judge: Bitmap<64>,
    /// 1: use
    /// 0: ignore
    umask: Bitmap<64>,
}

impl Judges {
    pub fn new(size: usize) -> Self {
        let mut mask = Bitmap::default();
        mask.invert();
        Self::with_mask(size, mask)
    }

    pub fn with_mask(size: usize, mask: Bitmap<64>) -> Self {
        let mut umask = mask.bitand(Bitmap::from_value(1u64.wrapping_shl(size as u32).wrapping_sub(1)));
        umask.invert();

        Self {
            judge: Bitmap::default(),
            umask,
        }
    }

    pub fn get(&mut self, index: usize) -> bool {
        self.judge.get(index)
    }

    pub fn set(&mut self, index: usize, judge: bool) {
        self.judge.set(index, judge);
    }

    pub fn apply(&mut self, judge: Bitmap<64>) {
        self.judge = judge;
    }

    pub fn set_mask(&mut self, mut mask: Bitmap<64>) {
        mask.invert();
        self.umask = mask;
    }

    /// judge | ( ~mask )
    ///
    /// | judge | mask | ~mask | result |
    /// |   0   |  0   |   1   |    1   |
    /// |   0   |  1   |   0   |    0   |
    /// |   1   |  0   |   1   |    1   |
    /// |   1   |  1   |   0   |    1   |
    pub fn judge(&self) -> bool {
        self.judge.bitor(self.umask).is_full()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_judge() {
        let mask = Bitmap::from_value(0b_0011);

        let dataset = vec![
            (mask, Bitmap::from_value(0b_0011), true),
            (mask, Bitmap::from_value(0b_1011), true),
            (mask, Bitmap::from_value(0b_1010), false),
        ];

        for (i, (mask, j, e)) in dataset.into_iter().enumerate() {
            let mut judge = Judges::with_mask(4, mask);
            judge.apply(j);
            assert_eq!(judge.judge(), e, "case {}, {} != {}", i, judge.judge(), e);
        }
    }
}
