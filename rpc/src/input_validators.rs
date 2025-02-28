use {
    std::{
        fmt::Display,
        str::FromStr
    },
};

fn is_parsable_generic<U, T>(string: T) -> Result<(), String>
where
    T: AsRef<str> + Display,
    U: FromStr,
    U::Err: Display,
{
    string
        .as_ref()
        .parse::<U>()
        .map(|_| ())
        .map_err(|err| format!("error parsing '{string}': {err}"))
}

// Return an error if string cannot be parsed as type T.
// Takes a String to avoid second type parameter when used as a clap validator
pub fn is_parsable<T>(string: String) -> Result<(), String>
where
    T: FromStr,
    T::Err: Display,
{
    is_parsable_generic::<T, String>(string)
}
