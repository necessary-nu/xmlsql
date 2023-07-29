mod builder;
mod document;
pub mod model;
mod parse;
mod select;

use std::{io::BufRead, path::Path};

pub use document::{DocumentDb, NodeType};
pub use parse::{Error, ParseOptions};
pub use select::Selector;

pub fn parse_to_disk<R: BufRead, P: AsRef<Path>>(
    db_path: P,
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create(db_path.as_ref())?;
    parse::parse(db, input, options)
}

pub fn parse_to_temp_file<R: BufRead>(
    input: R,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_temp()?;
    parse::parse(db, input, options)
}

pub fn parse_in_memory<R: BufRead>(input: R, options: ParseOptions) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_in_memory()?;
    parse::parse(db, input, options)
}
