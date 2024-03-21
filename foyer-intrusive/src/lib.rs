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

#![feature(associated_type_bounds)]
#![feature(ptr_metadata)]
#![feature(trait_alias)]
#![feature(lint_reasons)]
#![expect(clippy::new_without_default)]

pub use memoffset::offset_of;

/// Unsafe macro to get a raw pointer to an outer object from a pointer to one
/// of its fields.
///
/// # Examples
///
/// ```
/// use foyer_intrusive::container_of;
///
/// struct S { x: u32, y: u32 };
/// let container = S { x: 1, y: 2 };
/// let field = &container.x;
/// let container2: *const S = unsafe { container_of!(field, S, x) };
/// assert_eq!(&container as *const S, container2);
/// ```
///
/// # Safety
///
/// This is unsafe because it assumes that the given expression is a valid
/// pointer to the specified field of some container type.
#[macro_export]
macro_rules! container_of {
    ($ptr:expr, $container:path, $field:ident) => {
        ($ptr as *const _ as *const u8).sub($crate::offset_of!($container, $field)) as *const $container
    };
}

pub mod collections;
pub mod core;
pub mod eviction;
