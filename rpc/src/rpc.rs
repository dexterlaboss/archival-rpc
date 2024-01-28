
use {
    jsonrpc_core::{
        Error,
        Result
    },
    solana_rpc_client_api::{
        request::{
            MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
        },
    },
    solana_sdk::{
        commitment_config::{
            CommitmentConfig,
        },
        exit::Exit,
        pubkey::{
            Pubkey,
        },
        signature::{
            Signature,
        },
    },
    std::{
        sync::{
            atomic::{
                AtomicBool,
                Ordering
            },
            Arc,
            RwLock,
        },
    },
};

pub const MAX_REQUEST_BODY_SIZE: usize = 50 * (1 << 10); // 50kB
pub const PERFORMANCE_SAMPLES_LIMIT: usize = 720;


pub(crate) fn verify_and_parse_signatures_for_address_params(
    address: String,
    before: Option<String>,
    until: Option<String>,
    limit: Option<usize>,
) -> Result<(Pubkey, Option<Signature>, Option<Signature>, usize)> {
    let address = verify_pubkey(&address)?;
    let before = before
        .map(|ref before| verify_signature(before))
        .transpose()?;
    let until = until.map(|ref until| verify_signature(until)).transpose()?;
    let limit = limit.unwrap_or(MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT);

    if limit == 0 || limit > MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT {
        return Err(Error::invalid_params(format!(
            "Invalid limit; max {MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT}"
        )));
    }
    Ok((address, before, until, limit))
}

pub(crate) fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<()> {
    if !commitment.is_at_least_confirmed() {
        return Err(Error::invalid_params(
            "Method does not support commitment below `confirmed`",
        ));
    }
    Ok(())
}

pub fn create_rpc_service_exit(exit: &Arc<AtomicBool>) -> Arc<RwLock<Exit>> {
    let mut rpc_service_exit = Exit::default();
    let exit_ = exit.clone();
    rpc_service_exit.register_exit(Box::new(move || exit_.store(true, Ordering::Relaxed)));
    Arc::new(RwLock::new(rpc_service_exit))
}

pub(crate) fn verify_signature(input: &str) -> Result<Signature> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {e:?}")))
}

pub fn verify_pubkey(input: &str) -> Result<Pubkey> {
    input
        .parse()
        .map_err(|e| Error::invalid_params(format!("Invalid param: {e:?}")))
}