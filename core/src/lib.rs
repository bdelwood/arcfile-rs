// just use readme for docstring
#![doc = include_str!("../../README.md")]
pub mod arcfile;
pub mod error;
pub mod register;
pub mod regmap;

pub const MAX_RAYON_THREADS: usize = 12;
