#![allow(clippy::integer_arithmetic)]
#![recursion_limit = "2048"]

pub mod custom_error;

pub mod request_processor;
pub mod rpc_service;

#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;

#[cfg(test)]
#[macro_use]
extern crate serde_json;

pub mod cli;

pub mod logging;

pub mod middleware;

pub mod rpc;

pub mod rpc_server;

pub mod rpc_network_node;

#[macro_use]
pub mod rpc_network_info;

pub mod rpc_core;

pub mod input_validators;

pub mod deprecated;

#[cfg(test)]
#[macro_use]
extern crate matches;

