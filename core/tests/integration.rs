use arcfile::arcfile::{ArcFile, ArcFileLoader};
use arcfile::error::{ArcError, ArcResult};
use jiff::Timestamp;
use std::{path::PathBuf, str::FromStr};

use std::env;

fn init() {
    let _ = env_logger::builder()
        .is_test(true)
        .filter_level(log::LevelFilter::Trace)
        .try_init();
}

fn arcloader_fixture(t1: Option<&str>, t2: Option<&str>, filters: &[&str]) -> ArcResult<ArcFile> {
    init();
    let test_dir = PathBuf::from(env::var("ARCFILE_TEST_DIR").expect("ARCFILE_TEST_DIR not set"));
    let start = t1
        .map(|s| Timestamp::from_str(s).unwrap())
        .unwrap_or(Timestamp::MIN);
    let end = t2
        .map(|s| Timestamp::from_str(s).unwrap())
        .unwrap_or(Timestamp::MAX);

    let loader = ArcFileLoader::new(start..=end, filters)?;
    loader.load(&[test_dir])
}

#[test]
fn load_no_args() {
    let mut af = arcloader_fixture(None, None, &[]).unwrap();
    // check a feedback register
    let reg = af.get("mce0.data.fb").unwrap();
    assert!(reg.data().unwrap().nsamp > 0);
    // no filter, so we expect all the maps
    // run this last since into_tree takes the data
    assert_eq!(af.into_tree().len(), 6);
}

#[test]
fn load_with_filter() {
    let filt1 = "array.frame.utc";
    let filt2 = "antenna0.pmac.fast_enc_pos";
    let filt_wildcard = "mce*.data.fb";
    let filt_chsel = "mce0.data.fb[1]";
    let filt_chsel_repeat = "mce0.data.fb[1,2,1]";

    let af = arcloader_fixture(None, None, &[filt1, filt2]).unwrap();

    // should only have 2 registers
    assert_eq!(af.registers.len(), 2);
    assert!(af.get(filt1).is_ok());
    assert!(af.get(filt2).is_ok());

    // just mce registers
    let af = arcloader_fixture(None, None, &[filt_wildcard]).unwrap();
    assert_eq!(af.registers.len(), 4);
    // check all registers start with mce
    assert!(af.register_names().iter().all(|s| s.starts_with("mce")));

    // now test chsel
    let af = arcloader_fixture(None, None, &[filt_chsel]).unwrap();
    assert_eq!(af.registers.len(), 1);
    let reg = af.get("mce0.data.fb").unwrap();
    assert_eq!(reg.data().unwrap().nchan, 1);
    assert_eq!(reg.data().unwrap().nsamp, 60);

    // repeat channels in the filter should be deduped
    let af = arcloader_fixture(None, None, &[filt_chsel_repeat]).unwrap();
    assert_eq!(af.registers.len(), 1);
    let reg = af.get("mce0.data.fb").unwrap();
    assert_eq!(reg.data().unwrap().nchan, 2);
    assert_eq!(reg.data().unwrap().nsamp, 60);
}

#[test]
fn load_with_single_time_bound() {
    let tearly = Some("2099-01-01T00:00:00Z");
    let tlate = Some("2000-01-01T00:00:00Z");

    // start time bound
    let af = arcloader_fixture(tearly, None, &[]).unwrap();
    // we intentionally find 1 file before
    assert!(!af.registers.is_empty());
    let af = arcloader_fixture(tlate, None, &[]).unwrap();
    assert!(!af.registers.is_empty());

    // end time bound
    let af = arcloader_fixture(None, Some("2099-01-01T00:00:00Z"), &[]).unwrap();
    assert!(!af.registers.is_empty());
    // this should actually error, since start > end
    let result = arcloader_fixture(None, Some("2000-01-01T00:00:00Z"), &[]);
    assert!(matches!(result, Err(ArcError::Format(_))));
}

#[test]
fn load_with_both_bounds() {
    let af = arcloader_fixture(
        Some("2020-01-01T00:00:00Z"),
        Some("2050-01-02T00:00:00Z"),
        &[],
    )
    .unwrap();

    let nsamp_bounded = af.get("mce0.data.fb").unwrap().data().unwrap().nsamp;

    // wide window
    let af_all = arcloader_fixture(None, None, &[]).unwrap();
    let nsamp_all = af_all.get("mce0.data.fb").unwrap().data().unwrap().nsamp;

    // bounded should load <= all
    assert!(nsamp_bounded <= nsamp_all);
    assert!(nsamp_bounded > 0);
}

#[test]
fn load_with_both_bounds_and_filter() {
    let af = arcloader_fixture(
        Some("2024-01-01T00:00:00Z"),
        Some("2024-01-02T00:00:00Z"),
        &["mce0.data.fb[0:5]"],
    )
    .unwrap();

    assert_eq!(af.registers.len(), 1);

    let reg = af.get("mce0.data.fb").unwrap();
    let data = reg.data().unwrap();
    // channels 0..=5
    assert!(data.nsamp > 0);
    assert_eq!(data.nchan, 6);
}

#[test]
fn load_with_bad_filter() {
    let filt_bad_format = "mce0.data.fb[1";
    let filt_out_of_order = "mce0.data.fb[2:1]";

    // should raise error because of missing ]
    let af = arcloader_fixture(None, None, &[filt_bad_format]);
    assert!(matches!(af, Err(ArcError::InvalidInput(_))));
    // should raise error for nonsensical detector range
    let af = arcloader_fixture(None, None, &[filt_out_of_order]);

    assert!(matches!(af, Err(ArcError::InvalidInput(_))));
}
