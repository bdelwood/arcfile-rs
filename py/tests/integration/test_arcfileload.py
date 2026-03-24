import logging

import pytest

from arcfile import ArcFile
from datetime import datetime, timezone
import os


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
    assert all([key.startswith("mce") for key in af.keys()])

    # now check chsel works
    af = load(None, None, [filt_chsel])
    assert len(af) == 1
    value = af
    for key in filt_chsel.split("."):
        value = value[key.split("[", 1)[0]]
        assert value is not None
    assert value.shape == (60000,)

    # repeat channels in filter should be deduped
    af = load(None, None, [filt_chsel_repeat])
    value = af
    assert len(af) == 1
    for key in filt_chsel_repeat.split("."):
        value = value[key.split("[", 1)[0]]
        assert value is not None
    assert value.shape == (60000, 2)


def test_load_with_single_time_bound(load):
    tearly = parse_iso("2000-01-01T00:00:00Z")
    tlate = parse_iso("2099-01-01T00:00:00Z")

    # start time bound
    # both should not be empty
    # empty dict in Python is falsy
    af = load(tearly, None, None)
    assert af.to_dict()
    af = load(tlate, None, None)
    assert af.to_dict()

    # end time bound
    af = load(None, tlate, None)
    assert af.to_dict()

    with pytest.raises(OSError):
        af = load(None, tearly, None)


def test_load_with_both_bounds(load):
    t1 = parse_iso("2020-01-01T00:00:00Z")
    t2 = parse_iso("2050-01-02T00:00:00Z")
    af = load(t1, t2, None)

    nsamp_bounded = len(af["mce0"]["data"]["fb"])

    # wider window
    af = load(None, None, None)
    nsamp_all = len(af["mce0"]["data"]["fb"])

    assert nsamp_bounded <= nsamp_all
    assert nsamp_bounded > 0


def test_load_with_both_bounds_and_filter(load):
    t1 = parse_iso("2024-01-01T00:00:00Z")
    t2 = parse_iso("2024-01-02T00:00:00Z")
    filters = ["mce0.data.fb[0:5]"]

    # should pick up one file from "one-file-before" behavior
    af = load(t1, t2, filters)

    (nsamp, nchan) = af["mce0"]["data"]["fb"].shape

    # should only have 6 channels, 0:5
    assert nsamp > 0
    assert nchan == 6
