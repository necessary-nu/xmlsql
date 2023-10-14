mod builder;
mod document;
mod infer;
pub mod model;
mod parse;
pub mod redact;
mod select;
mod writer;

use std::path::Path;

pub use document::{DocumentDb, NodeType};
pub use infer::{Inferred, InferredType};
pub use parse::{Error, ParseOptions};
pub use select::Selector;

pub fn parse_path_to_disk<P: AsRef<Path>>(
    db_path: P,
    path: &Path,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let f = unsafe { memmap2::Mmap::map(&std::fs::File::open(path).unwrap()).unwrap() };
    f.advise(memmap2::Advice::Sequential).unwrap();
    let s = unsafe { std::str::from_utf8_unchecked(&f) };
    parse_to_disk(db_path, s, options)
}

pub fn parse_path_in_memory<P: AsRef<Path>>(
    path: &Path,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let f = unsafe { memmap2::Mmap::map(&std::fs::File::open(path).unwrap()).unwrap() };
    f.advise(memmap2::Advice::Sequential).unwrap();
    let s = unsafe { std::str::from_utf8_unchecked(&f) };
    parse_in_memory(s, options)
}

pub fn parse_path_to_temp_file(path: &Path, options: ParseOptions) -> Result<DocumentDb, Error> {
    let f = unsafe { memmap2::Mmap::map(&std::fs::File::open(path).unwrap()).unwrap() };
    f.advise(memmap2::Advice::Sequential).unwrap();
    let s = unsafe { std::str::from_utf8_unchecked(&f) };
    parse_to_temp_file(s, options)
}

pub fn parse_to_disk<P: AsRef<Path>>(
    db_path: P,
    input: &str,
    options: ParseOptions,
) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create(db_path.as_ref(), options.infer_types)?;
    parse::parse(db, input, options)
}

pub fn parse_to_temp_file(input: &str, options: ParseOptions) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_temp(options.infer_types)?;
    parse::parse(db, input, options)
}

pub fn parse_in_memory(input: &str, options: ParseOptions) -> Result<DocumentDb, Error> {
    let db = DocumentDb::create_in_memory(options.infer_types)?;
    parse::parse(db, input, options)
}
