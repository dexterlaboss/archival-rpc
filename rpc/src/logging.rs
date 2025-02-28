#![allow(clippy::integer_arithmetic)]
use {
    std::{
        env,
        fs::{OpenOptions},
        process::exit,
        thread::JoinHandle,
    },
};

#[cfg(unix)]
fn redirect_stderr(filename: &str) {
    use std::os::unix::io::AsRawFd;
    match OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(filename)
    {
        Ok(file) => unsafe {
            libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
        },
        Err(err) => eprintln!("Unable to open {filename}: {err}"),
    }
}

// Redirect stderr to a file with support for logrotate by sending a SIGUSR1 to the process.
//
// Upon success, future `log` macros and `eprintln!()` can be found in the specified log file.
pub fn redirect_stderr_to_file(logfile: Option<String>) -> Option<JoinHandle<()>> {
    // Default to RUST_BACKTRACE=1 for more informative launcher logs
    if env::var_os("RUST_BACKTRACE").is_none() {
        env::set_var("RUST_BACKTRACE", "1")
    }

    let filter = "solana=info";
    match logfile {
        None => {
            solana_logger::setup_with_default(filter);
            None
        }
        Some(logfile) => {
            #[cfg(unix)]
            {
                use log::info;
                let mut signals =
                    signal_hook::iterator::Signals::new([signal_hook::consts::SIGUSR1])
                        .unwrap_or_else(|err| {
                            eprintln!("Unable to register SIGUSR1 handler: {err:?}");
                            exit(1);
                        });

                solana_logger::setup_with_default(filter);
                redirect_stderr(&logfile);
                Some(
                    std::thread::Builder::new()
                        .name("solSigUsr1".into())
                        .spawn(move || {
                            for signal in signals.forever() {
                                info!(
                                    "received SIGUSR1 ({}), reopening log file: {:?}",
                                    signal, logfile
                                );
                                redirect_stderr(&logfile);
                            }
                        })
                        .unwrap(),
                )
            }
            #[cfg(not(unix))]
            {
                println!("logrotate is not supported on this platform");
                solana_logger::setup_file_with_default(&logfile, filter);
                None
            }
        }
    }
}

