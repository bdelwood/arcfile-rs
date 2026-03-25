import logging
import os

import pytest
from datetime import datetime, timezone

from arcfile import ArcFile


try:
    TEST_DIR = os.environ["ARCFILE_TEST_DIR"]
except KeyError as e:
    raise RuntimeError(
        "Required environment variable ARCFILE_TEST_DIR is not set"
    ) from e

logging.basicConfig(level=logging.DEBUG)


@pytest.fixture()
def load():
    def _load(t1=None, t2=None, filters=None):
        return ArcFile.load(TEST_DIR, t1, t2, filters)

    return _load


def parse_iso(s):
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def test_load_no_args(load):
    af = load()

    # len is number of top-level maps
    assert len(af) == 6

    reg = af["mce0"]["data"]["fb"]
    assert reg.ndim == 2
    assert len(reg) > 0


def test_load_with_filter(load):
    filt1 = "array.frame.utc"
    filt2 = "antenna0.pmac.fast_enc_pos"
    filt_wildcard = "mce*.data.fb"
    filt_chsel = "mce0.data.fb[1]"
    filt_chsel_repeat = "mce0.data.fb[1,2,1]"

    # should only have 2 registers
    af = load(None, None, filters=[filt1, filt2])
    assert len(af) == 2
    value = af
    for key in filt1.split("."):
        value = value[key]
        assert value is not None

    # just mce registers
    af = load(None, None, [filt_wildcard])
    assert len(af) == 4
    af = af.to_dict()
    assert all(key.startswith("mce") for key in af.keys())

    # now check chsel works
    af = load(None, None, [filt_chsel])
    assert len(af) == 1
    value = af
    for key in filt_chsel.split("."):
        value = value[key.split("[", 1)[0]]
        assert value is not None
    assert value.shape == (60,)

    # repeat channels in filter should be deduped
    af = load(None, None, [filt_chsel_repeat])
    assert len(af) == 1
    value = af
    for key in filt_chsel_repeat.split("."):
        value = value[key.split("[", 1)[0]]
        assert value is not None
    assert value.shape == (60, 2)


def test_load_with_filter_exclusion(load):
    # first-match: err[] excludes err, * includes everything else
    af = load(None, None, ["mce*.data.err[]", "*"])
    assert len(af) > 0
    assert "err" not in af["mce0"]["data"].keys()
    assert af["mce0"]["data"]["fb"] is not None
    assert len(af["mce0"]["data"]["fb"]) > 0

    # channel selection with exclusion: err excluded, fb gets 5 channels, rest gets all
    af = load(None, None, ["mce*.data.err[]", "mce0.data.fb[0:4]", "*"])
    assert "err" not in af["mce0"]["data"].keys()
    fb = af["mce0"]["data"]["fb"]
    assert fb.shape[1] == 5  # channels 0,1,2,3,4
    assert af["array"]["frame"]["utc"] is not None


def test_load_with_single_time_bound(load):
    tearly = parse_iso("2000-01-01T00:00:00Z")
    tlate = parse_iso("2099-01-01T00:00:00Z")

    # start time bound: file is in range
    af = load(tearly, None, None)
    assert af.to_dict()

    # start time bound: file too far in past, returns empty
    af = load(tlate, None, None)
    assert not af.to_dict()

    # end time bound: file is in range
    af = load(None, tlate, None)
    assert af.to_dict()

    # start > end should error
    with pytest.raises(OSError):
        load(
            parse_iso("2026-01-01T00:00:00Z"),
            parse_iso("2000-01-01T00:00:00Z"),
            None,
        )


def test_load_with_both_bounds(load):
    t1 = parse_iso("2018-01-01T00:00:00Z")
    t2 = parse_iso("2050-01-02T00:00:00Z")
    af = load(t1, t2, None)

    nsamp_bounded = len(af["mce0"]["data"]["fb"])

    # wider window
    af = load(None, None, None)
    nsamp_all = len(af["mce0"]["data"]["fb"])

    assert nsamp_bounded <= nsamp_all
    assert nsamp_bounded > 0


def test_load_with_both_bounds_and_filter(load):
    # out-of-range times: returns empty, not error
    af = load(
        parse_iso("2024-01-01T00:00:00Z"),
        parse_iso("2024-01-02T00:00:00Z"),
        ["mce0.data.fb[0:5]"],
    )
    assert not af.to_dict()

    af = load(
        parse_iso("2018-01-01T00:00:00Z"),
        parse_iso("2024-01-02T00:00:00Z"),
        ["mce0.data.fb[0:5]"],
    )

    (nsamp, nchan) = af["mce0"]["data"]["fb"].shape
    assert nsamp > 0
    assert nchan == 6


def test_load_out_of_range_returns_empty(load):
    # dates far in the past: no files match, returns empty
    af = load(
        parse_iso("2000-01-01T00:00:00Z"),
        parse_iso("2000-01-02T00:00:00Z"),
        None,
    )
    assert not af.to_dict()

    # dates far in the future: same
    af = load(
        parse_iso("2099-01-01T00:00:00Z"),
        parse_iso("2099-01-02T00:00:00Z"),
        None,
    )
    assert not af.to_dict()
