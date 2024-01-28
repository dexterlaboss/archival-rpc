#![allow(clippy::integer_arithmetic)]
pub mod custom_error;

pub mod rpc;

pub mod storage_rpc;
pub mod storage_rpc_service;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

