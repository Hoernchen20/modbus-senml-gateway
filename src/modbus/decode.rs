use crate::config::schema::{BlockConfig, DataType, WordOrder};

pub fn decode(block: &BlockConfig, words: &[u16]) -> Vec<(String, f64)> {
    block
        .points
        .iter()
        .map(|p| {
            let raw = extract_raw(words, p.offset, p.data_type, block.word_order);
            let mut value = raw * p.scale;
            if p.absolute {
                value = value.abs();
            }
            (p.name.clone(), value)
        })
        .collect()
}

fn extract_raw(words: &[u16], offset: u16, data_type: DataType, word_order: WordOrder) -> f64 {
    let idx = offset as usize;
    match data_type {
        DataType::U16 => words[idx] as f64,
        DataType::I16 => words[idx] as i16 as f64,
        DataType::U32 => combine_u32(words, idx, word_order) as f64,
        DataType::I32 => combine_u32(words, idx, word_order) as i32 as f64,
        DataType::F32 => f32::from_bits(combine_u32(words, idx, word_order)) as f64,
    }
}

/// Combines two consecutive 16-bit registers into a 32-bit value per the
/// block's word order: `big_endian` means the first (lower-address) register
/// holds the high-order 16 bits, `little_endian` swaps that.
fn combine_u32(words: &[u16], idx: usize, word_order: WordOrder) -> u32 {
    let (hi, lo) = match word_order {
        WordOrder::BigEndian => (words[idx], words[idx + 1]),
        WordOrder::LittleEndian => (words[idx + 1], words[idx]),
    };
    ((hi as u32) << 16) | (lo as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::schema::{Function, PointConfig};

    fn point(offset: u16, data_type: DataType, scale: f64, absolute: bool) -> PointConfig {
        PointConfig {
            name: "p".to_string(),
            offset,
            data_type,
            scale,
            absolute,
            unit: "unit".to_string(),
        }
    }

    fn block(word_order: WordOrder, points: Vec<PointConfig>) -> BlockConfig {
        BlockConfig {
            function: Function::Holding,
            start: 0,
            count: 8,
            word_order,
            points,
        }
    }

    fn decode_one(word_order: WordOrder, words: &[u16], p: PointConfig) -> f64 {
        decode(&block(word_order, vec![p]), words)[0].1
    }

    #[test]
    fn u16_decodes_unsigned() {
        let v = decode_one(WordOrder::BigEndian, &[65535], point(0, DataType::U16, 1.0, false));
        assert_eq!(v, 65535.0);
    }

    #[test]
    fn i16_decodes_signed() {
        // 0xFFFF as i16 is -1.
        let v = decode_one(WordOrder::BigEndian, &[0xFFFF], point(0, DataType::I16, 1.0, false));
        assert_eq!(v, -1.0);
    }

    #[test]
    fn u32_big_endian_high_word_first() {
        // 0x0001_0000 = 65536, high word first.
        let v = decode_one(
            WordOrder::BigEndian,
            &[0x0001, 0x0000],
            point(0, DataType::U32, 1.0, false),
        );
        assert_eq!(v, 65536.0);
    }

    #[test]
    fn u32_little_endian_swaps_words() {
        // Same registers as above, but little_endian reads the low word first.
        let v = decode_one(
            WordOrder::LittleEndian,
            &[0x0001, 0x0000],
            point(0, DataType::U32, 1.0, false),
        );
        assert_eq!(v, 1.0);
    }

    #[test]
    fn i32_decodes_negative() {
        let v = decode_one(
            WordOrder::BigEndian,
            &[0xFFFF, 0xFFFF],
            point(0, DataType::I32, 1.0, false),
        );
        assert_eq!(v, -1.0);
    }

    #[test]
    fn f32_decodes_ieee754() {
        let bits = 1.5f32.to_bits();
        let hi = (bits >> 16) as u16;
        let lo = (bits & 0xFFFF) as u16;
        let v = decode_one(
            WordOrder::BigEndian,
            &[hi, lo],
            point(0, DataType::F32, 1.0, false),
        );
        assert_eq!(v, 1.5);
    }

    #[test]
    fn scale_multiplies_raw_value() {
        let v = decode_one(WordOrder::BigEndian, &[1000], point(0, DataType::U16, 0.1, false));
        assert!((v - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn absolute_false_leaves_negative_untouched() {
        let v = decode_one(
            WordOrder::BigEndian,
            &[0xFF9C], // -100 as i16
            point(0, DataType::I16, 1.0, false),
        );
        assert_eq!(v, -100.0);
    }

    #[test]
    fn absolute_true_flips_negative_to_positive() {
        let v = decode_one(
            WordOrder::BigEndian,
            &[0xFF9C], // -100 as i16
            point(0, DataType::I16, 1.0, true),
        );
        assert_eq!(v, 100.0);
    }

    #[test]
    fn negative_scale_then_absolute_is_positive() {
        // scale flips the sign first, then .abs() is applied after.
        let v = decode_one(WordOrder::BigEndian, &[100], point(0, DataType::U16, -1.0, true));
        assert_eq!(v, 100.0);
    }

    #[test]
    fn multiple_points_in_one_block() {
        let block = block(
            WordOrder::BigEndian,
            vec![
                point(0, DataType::U16, 1.0, false),
                point(1, DataType::U16, 1.0, false),
            ],
        );
        let readings = decode(&block, &[10, 20]);
        assert_eq!(readings, vec![("p".to_string(), 10.0), ("p".to_string(), 20.0)]);
    }
}
