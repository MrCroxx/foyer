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

#![feature(allocator_api)]
#![feature(strict_provenance)]
#![feature(trait_alias)]
#![feature(get_mut_unchecked)]
#![feature(let_chains)]
#![feature(error_generic_member_access)]
#![feature(lazy_cell)]
#![feature(lint_reasons)]
#![feature(associated_type_defaults)]
#![feature(box_into_inner)]
#![feature(try_trait_v2)]

pub mod admission;
pub mod buffer;
pub mod catalog;
pub mod compress;
pub mod device;
pub mod error;
pub mod flusher;
pub mod generic;
pub mod judge;
pub mod lazy;
pub mod metrics;
pub mod reclaimer;
pub mod region;
pub mod region_manager;
pub mod reinsertion;
pub mod runtime;
pub mod storage;
pub mod store;
pub mod statistics;

pub mod test_utils;
