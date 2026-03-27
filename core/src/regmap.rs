use crate::error::{ArcError, ArcResult};
use bitflags::bitflags;

use byteorder::{BigEndian, LittleEndian, ReadBytesExt};
use std::io::{Cursor, Read};

#[derive(Debug, Clone)]
pub struct RegBlockSpec {
    pub map_name: String,
    pub board_name: String,
    pub block_name: String,
    pub typeword: TypeWord,
    pub nchan: usize,
    pub spf: usize,
    pub ofs: usize,
}

impl RegBlockSpec {
    // replaces add_regblock in RWO's implementation.
    pub fn new(
        map_name: String,
        board_name: String,
        block_name: String,
        spec: [u32; 6],
        ofs: usize,
    ) -> ArcResult<Self> {
        let typeword = TypeWord::try_from(spec[0])?;
        let nchan = spec[4] as usize;
        let spf = spec[5] as usize;

        // copy of lines 170-177 in reglist.
        // swaps nchan and spf based on fast flag?
        let (nchan, spf) = if typeword.flags.fast() && spf == 0 {
            (1, nchan)
        } else if !typeword.flags.fast() {
            (nchan, 1)
        } else {
            (nchan, spf)
        };

        Ok(Self {
            map_name,
            board_name,
            block_name,
            typeword,
            nchan,
            spf,
            ofs,
        })
    }

    pub fn is_fast(&self) -> bool {
        self.typeword.flags.fast()
    }

    pub fn do_arc(&self) -> bool {
        !self.typeword.flags.excluded()
    }

    pub fn element_size(&self) -> usize {
        self.typeword.element_size()
    }

    pub fn frame_size(&self) -> usize {
        self.typeword.frame_size(self.nchan, self.spf)
    }

    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.map_name, self.board_name, self.block_name)
    }

    pub fn type_name(&self) -> &'static str {
        self.typeword.reg_type.name()
    }
}

// Types for regmap
bitflags! {
     #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
     pub struct RegFlag: u32 {
        const COMPLEX = 0x1;
        const R = 0x4;
        const W = 0x8;
        const RW = Self::R.bits() | Self::W.bits();
        const PREAVG = 0x10;
        const POSTAVG = 0x20;
        const SUM = 0x40;
        const UNION = 0x80;
        const EXC = 0x100;
        const PCI = 0x400;
        const DPRAM = 0x100000;
        const FAST = 0x200000;
        const FIRFILT = 0x400000;
        const MAX = 0x800000;
     }
}

impl RegFlag {
    fn excluded(&self) -> bool {
        self.contains(Self::EXC)
    }

    fn complex(&self) -> bool {
        self.contains(Self::COMPLEX)
    }

    fn fast(&self) -> bool {
        self.contains(Self::FAST)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegType {
    Utc,
    Bool,
    Char,
    UChar,
    Short,
    UShort,
    Int,
    UInt,
    Float,
    Double,
}

impl RegType {
    pub fn element_size(&self) -> usize {
        // from databuf.c
        match self {
            Self::Bool | Self::Char | Self::UChar => 1,
            Self::Short | Self::UShort => 2,
            Self::Int | Self::UInt | Self::Float => 4,
            Self::Utc | Self::Double => 8,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Utc => "utc",
            Self::Bool => "bool",
            Self::Char => "int8",
            Self::UChar => "uint8",
            Self::Short => "int16",
            Self::UShort => "uint16",
            Self::Int => "int32",
            Self::UInt => "uint32",
            Self::Float => "float32",
            Self::Double => "float64",
        }
    }
}

impl From<RegType> for u32 {
    fn from(reg_type: RegType) -> Self {
        match reg_type {
            RegType::Utc => 0x200,
            // included for completeness, but the C implementation effectively ignores this in `mex_readarc`, line 165
            RegType::Bool => 0x800,
            RegType::Char => 0x1000,
            RegType::UChar => 0x2000,
            RegType::Short => 0x4000,
            RegType::UShort => 0x8000,
            RegType::Int => 0x10000,
            RegType::UInt => 0x20000,
            RegType::Float => 0x40000,
            RegType::Double => 0x80000,
        }
    }
}

const TYPE_MASK: u32 =
    0x200 | 0x800 | 0x1000 | 0x2000 | 0x4000 | 0x8000 | 0x10000 | 0x20000 | 0x40000 | 0x80000;

impl TryFrom<u32> for RegType {
    type Error = ArcError;

    fn try_from(type_word: u32) -> ArcResult<Self> {
        let type_bits = type_word & TYPE_MASK;

        // error if no type bits
        if type_bits == 0 {
            return Err(ArcError::UnknownRegFlag(type_word));
        };

        if type_bits.count_ones() > 1 {
            return Err(ArcError::Corrupted("Multiple type flags".to_string()));
        };

        match type_bits {
            0x200 => Ok(Self::Utc),
            0x800 => Ok(Self::Bool),
            0x1000 => Ok(Self::Char),
            0x2000 => Ok(Self::UChar),
            0x4000 => Ok(Self::Short),
            0x8000 => Ok(Self::UShort),
            0x10000 => Ok(Self::Int),
            0x20000 => Ok(Self::UInt),
            0x40000 => Ok(Self::Float),
            0x80000 => Ok(Self::Double),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TypeWord {
    pub reg_type: RegType,
    flags: RegFlag,
}

impl TypeWord {
    fn element_size(&self) -> usize {
        if self.flags.excluded() {
            return 0;
        }

        let base = self.reg_type.element_size();
        // from element_size of databuf.c
        if self.flags.complex() { 2 * base } else { base }
    }

    pub fn frame_size(&self, nchan: usize, spf: usize) -> usize {
        // max(1) so that unspecified nchan/spf are size 1 not 0
        self.element_size() * nchan.max(1) * spf.max(1)
    }
}

impl TryFrom<u32> for TypeWord {
    type Error = ArcError;

    fn try_from(tw: u32) -> ArcResult<Self> {
        Ok(Self {
            reg_type: RegType::try_from(tw)?,
            flags: RegFlag::from_bits_truncate(tw),
        })
    }
}

impl From<TypeWord> for u32 {
    fn from(tw: TypeWord) -> Self {
        u32::from(tw.reg_type) | tw.flags.bits()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum FrameRecord {
    Status,
    Received,
    Nsnap,
    Record,
    Utc,
    Lst,
    Features,
    MarkSeq,
}

impl FrameRecord {
    fn name(&self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::Received => "received",
            Self::Nsnap => "nsnap",
            Self::Record => "record",
            Self::Utc => "utc",
            Self::Lst => "lst",
            Self::Features => "features",
            Self::MarkSeq => "markSeq",
        }
    }

    // fn reg_type(&self) -> RegType {}
}

impl From<&FrameRecord> for RegType {
    fn from(fr: &FrameRecord) -> Self {
        match fr {
            FrameRecord::Status => Self::UInt,
            FrameRecord::Received => Self::UChar,
            FrameRecord::Nsnap => Self::UInt,
            FrameRecord::Record => Self::UInt,
            FrameRecord::Utc => Self::Utc,
            FrameRecord::Lst => Self::UInt,
            FrameRecord::Features => Self::UInt,
            FrameRecord::MarkSeq => Self::UInt,
        }
    }
}

impl RegBlockSpec {
    fn from_frame_board(map_name: String, frame: &FrameRecord, ofs: &mut usize) -> ArcResult<Self> {
        // TODO: chaining here seems kind of awkward--better to implement direct to u32 for frames?
        let typeword = u32::from(RegType::from(frame)) | RegFlag::empty().bits();
        let spec = [typeword, 0x0F, 0, 0, 1, 0];

        Self::new(map_name, "frame".into(), frame.name().into(), spec, *ofs)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Endianness {
    Big,
    Little,
}

impl Endianness {
    pub fn native() -> Self {
        if cfg!(target_endian = "little") {
            Self::Little
        } else {
            Self::Big
        }
    }

    pub fn swap(&self) -> Self {
        match self {
            Self::Big => Self::Little,
            Self::Little => Self::Big,
        }
    }
}

impl Default for Endianness {
    fn default() -> Self {
        Self::native()
    }
}

// essentially replaces MemBuf with Rust native objects
pub struct RegMapReader<'a> {
    cursor: Cursor<&'a [u8]>,
    endianness: Endianness,
}

impl<'a> RegMapReader<'a> {
    pub fn new(data: &'a [u8], endianness: Endianness) -> Self {
        Self {
            cursor: Cursor::new(data),
            endianness,
        }
    }

    pub fn read_u16(&mut self) -> ArcResult<u16> {
        match self.endianness {
            Endianness::Big => Ok(self.cursor.read_u16::<BigEndian>()?),
            Endianness::Little => Ok(self.cursor.read_u16::<LittleEndian>()?),
        }
    }

    pub fn read_u32(&mut self) -> ArcResult<u32> {
        match self.endianness {
            Endianness::Big => Ok(self.cursor.read_u32::<BigEndian>()?),
            Endianness::Little => Ok(self.cursor.read_u32::<LittleEndian>()?),
        }
    }

    pub fn read_name(&mut self) -> ArcResult<String> {
        let len = self.read_u16()? as usize;
        let mut buf = vec![0u8; len];
        self.cursor.read_exact(&mut buf)?;
        Ok(String::from_utf8(buf)?)
    }

    pub fn read_regblock_spec(&mut self) -> ArcResult<[u32; 6]> {
        let mut spec = [0u32; 6];

        for s in &mut spec {
            *s = self.read_u32()?;
        }
        Ok(spec)
    }

    pub fn check_zeros(&mut self, n: usize) -> ArcResult<()> {
        let mut buf = vec![0u8; n];
        self.cursor.read_exact(&mut buf)?;
        if buf.iter().any(|&b| b != 0) {
            return Err(ArcError::Corrupted("Expected zero padding".into()));
        }
        Ok(())
    }
}

const FRAME_BLOCKS: &[FrameRecord] = &[
    FrameRecord::Status,
    FrameRecord::Received,
    FrameRecord::Nsnap,
    FrameRecord::Record,
    FrameRecord::Utc,
    FrameRecord::Lst,
    FrameRecord::Features,
    FrameRecord::MarkSeq,
];

pub fn parse_regmap(data: &[u8], endianness: Endianness) -> ArcResult<Vec<RegBlockSpec>> {
    let mut reader = RegMapReader::new(data, endianness);
    let mut regs = Vec::new();
    let mut ofs: usize = 8;

    let num_maps = reader.read_u16()?;

    for _ in 0..num_maps {
        let map_name = reader.read_name()?;

        add_frame_board(&mut regs, &map_name, &mut ofs)?;

        let num_boards = reader.read_u16()?;

        for _ in 0..num_boards {
            let board_name = reader.read_name()?;

            add_status_regblock(&mut regs, &map_name, &board_name, &mut ofs)?;

            let num_blocks = reader.read_u16()?;

            for _ in 0..num_blocks {
                let block_name = reader.read_name()?;
                let spec = reader.read_regblock_spec()?;

                let reg =
                    RegBlockSpec::new(map_name.clone(), board_name.clone(), block_name, spec, ofs)?;
                ofs += reg.frame_size();
                regs.push(reg);
            }

            reader.check_zeros(16)?; // board padding, after each board
        }
    }

    Ok(regs)
}

fn add_frame_board(regs: &mut Vec<RegBlockSpec>, map_name: &str, ofs: &mut usize) -> ArcResult<()> {
    for board in FRAME_BLOCKS {
        let reg = RegBlockSpec::from_frame_board(map_name.into(), board, ofs)?;
        *ofs += reg.frame_size();

        regs.push(reg);
    }

    Ok(())
}

// each board has status register
// that's not in the file's register map
// we have to add it implicitly
fn add_status_regblock(
    regs: &mut Vec<RegBlockSpec>,
    map_name: &str,
    board_name: &str,
    ofs: &mut usize,
) -> ArcResult<()> {
    let typeword = u32::from(RegType::UInt) | RegFlag::empty().bits();
    // status register: GCP_REG_UINT, nchan=1, spf=0
    let spec = [typeword, 0x0F, 0, 0, 1, 0];

    let reg = RegBlockSpec::new(
        map_name.into(),
        board_name.into(),
        "status".into(),
        spec,
        *ofs,
    )?;

    *ofs += reg.frame_size();
    regs.push(reg);

    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    // some helpers to write to buffers
    fn write_u16(buf: &mut Vec<u8>, v: u16) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    fn write_u32(buf: &mut Vec<u8>, v: u32) {
        buf.extend_from_slice(&v.to_le_bytes());
    }

    fn write_name(buf: &mut Vec<u8>, name: &str) {
        write_u16(buf, name.len() as u16);
        buf.extend_from_slice(name.as_bytes());
    }

    fn write_regblock_spec(buf: &mut Vec<u8>, spec: [u32; 6]) {
        for s in &spec {
            write_u32(buf, *s);
        }
    }

    #[test]
    fn parse_regmap_round_trips_single_board() {
        // start constructing block
        let mut buf = Vec::new();

        // add map
        write_u16(&mut buf, 1);
        write_name(&mut buf, "array");

        // add board
        write_u16(&mut buf, 1);
        write_name(&mut buf, "mce0");

        // add block
        // of type UInt | FAST | RW
        // with nchan=3, spf=1
        write_u16(&mut buf, 1);
        write_name(&mut buf, "data");
        let typeword = 0x20000 | 0x200000 | 0x0C; // UInt | FAST | RW
        write_regblock_spec(&mut buf, [typeword, 0x0F, 0, 0, 3, 1]);

        // 16 bytes board padding
        buf.extend_from_slice(&[0u8; 16]);

        let regs = parse_regmap(&buf, Endianness::Little).unwrap();

        // 8 frame registers + 1 implicit status + 1 explicit block = 10
        assert_eq!(regs.len(), 10);

        // frame registers always included
        assert_eq!(regs[0].full_name(), "array.frame.status");
        assert_eq!(regs[7].full_name(), "array.frame.markSeq");

        // implicit board status register
        assert_eq!(regs[8].full_name(), "array.mce0.status");
        assert_eq!(regs[8].type_name(), "uint32");

        // assert block we made comes out as expected
        let data = &regs[9];
        assert_eq!(data.full_name(), "array.mce0.data");
        assert_eq!(data.type_name(), "uint32");
        assert_eq!(data.nchan, 3);
        assert_eq!(data.spf, 1);
        assert!(data.is_fast());

        // offsets should be cumulative
        assert!(data.ofs > regs[8].ofs);
    }

    #[test]
    fn typeword_try_from_rejects_multiple_type_bits() {
        let multi = 0x40000 | 0x80000; // Float + Double
        let result = TypeWord::try_from(multi);
        // make sure error is raised for bad flag combos
        assert!(result.is_err());
        match result.unwrap_err() {
            ArcError::Corrupted(msg) => assert!(msg.contains("Multiple type flags")),
            other => panic!("expected Corrupted, got: {other:?}"),
        }
    }

    #[test]
    fn frame_size_accounts_for_complex_flag() {
        let base_tw = 0x40000 | 0x20000C; // Float | FAST | RW
        let complex_tw = base_tw | 0x1; // + COMPLEX

        let base_spec = [base_tw, 0x0F, 0, 0, 4, 1];
        let complex_spec = [complex_tw, 0x0F, 0, 0, 4, 1];

        let base = RegBlockSpec::new("t".into(), "b".into(), "r".into(), base_spec, 0).unwrap();

        let complex =
            RegBlockSpec::new("t".into(), "b".into(), "r".into(), complex_spec, 0).unwrap();

        assert_eq!(complex.frame_size(), 2 * base.frame_size());
        assert_eq!(base.element_size(), 4); // f32
        assert_eq!(complex.element_size(), 8); // complex f32 should be double
    }
}
