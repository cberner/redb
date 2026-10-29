//! All of the structures in this module can be serialized to a fixed-width byte array.
//!
//! They are designed to be compatible with any time implemenation. All integers are using little endian. The layout of the memory is as follows:
//!
//!| Structure             | Layout                                                                                                                   | Size |                                                  |
//!|-----------------------|--------------------------------------------------------------------------------------------------------------------------|------|--------------------------------------------------|
//!| NaiveDate             | `{year:i32}\|{month:u8}\|{day:u8}`                                                                                       | 6    |                                                  |
//!| NaiveTime             | `{seconds_from_midnight:u32:first 3 bytes}\|{nanoseconds:u32}`                                                           | 7    |                                                  |
//!| NaiveDateTime         | `{year:i32}\|{month:u8}\|{day:u8}\|{seconds_from_midnight:u32:first 3 bytes}\|{nanoseconds:u32}`                         | 13   |                                                  |
//!| FixedOffset           | `{seconds_from_utc:i32}`                                                                                                 | 4    |                                                  |
//!| DateTime<FixedOffset> | `{year:i32}\|{month:u8}\|{day:u8}\|{seconds_from_midnight:u32:first 3 bytes}\|{nanoseconds:u32}\|{seconds_from_utc:i32}` | 17   | Time is stored in UTC with the offset in seconds |
use crate::types::{ValueError, value_error};
use crate::{Key, TypeName, Value};

use chrono_v0_4::{
    DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
};
/// A 6-byte representation of a date in the format `{year:i32}\|{month:u8}\|{day:u8}`.
impl Value for NaiveDate {
    type SelfType<'a>
        = NaiveDate
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 6]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(6)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        if data.len() != 6 {
            return Err(value_error(format_args!(
                "NaiveDate must be 6 bytes long, got {}",
                data.len()
            )));
        }
        date_from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let year = value.year().to_le_bytes();
        let month = u8::try_from(value.month()).unwrap();
        let day = u8::try_from(value.day()).unwrap();
        [year[0], year[1], year[2], year[3], month, day]
    }

    fn type_name() -> super::TypeName {
        TypeName::new("chrono::NaiveDate")
    }
}
impl Key for NaiveDate {
    fn compare(data1: &[u8], data2: &[u8]) -> core::cmp::Ordering {
        compare_decoded(data1, data2, |data| NaiveDate::try_from_bytes(data))
    }

    fn try_compare(
        data1: &[u8],
        data2: &[u8],
    ) -> core::result::Result<core::cmp::Ordering, ValueError> {
        try_compare_decoded(data1, data2, |data| NaiveDate::try_from_bytes(data))
    }
}
/// A 7-byte representation of a time in the format `{seconds_from_midnight:i32:first 3 bytes}\|{nanoseconds:i32}`.
///
/// Only the first 3 bytes of the seconds from midnight are stored, as the maximum value is 86,400 seconds (24 hours).
impl Value for NaiveTime {
    type SelfType<'a>
        = NaiveTime
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 7]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(7)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        if data.len() != 7 {
            return Err(value_error(format_args!(
                "NaiveTime must be 7 bytes long, got {}",
                data.len()
            )));
        }
        time_from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let num_seconds = value.num_seconds_from_midnight().to_le_bytes();
        let num_nanoseconds = value.nanosecond().to_le_bytes();
        [
            num_seconds[0],
            num_seconds[1],
            num_seconds[2],
            num_nanoseconds[0],
            num_nanoseconds[1],
            num_nanoseconds[2],
            num_nanoseconds[3],
        ]
    }

    fn type_name() -> TypeName {
        TypeName::new("chrono::NaiveTime")
    }
}
impl Key for NaiveTime {
    fn compare(data1: &[u8], data2: &[u8]) -> core::cmp::Ordering {
        compare_decoded(data1, data2, |data| NaiveTime::try_from_bytes(data))
    }

    fn try_compare(
        data1: &[u8],
        data2: &[u8],
    ) -> core::result::Result<core::cmp::Ordering, ValueError> {
        try_compare_decoded(data1, data2, |data| NaiveTime::try_from_bytes(data))
    }
}
/// A 13-byte representation of a date and time in the format `{year:i32}\|{month:u8}\|{day:u8}\|{seconds_from_midnight:i32:first 3 bytes}\|{nanoseconds:i32}`.
/// The date is stored as a `NaiveDate` and the time as a `NaiveTime`. The maximum value for seconds from midnight is 86,400 seconds (24 hours), so only the first 3 bytes are stored.
impl Value for NaiveDateTime {
    type SelfType<'a>
        = NaiveDateTime
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 13]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(13)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        if data.len() != 13 {
            return Err(value_error(format_args!(
                "NaiveDateTime must be 13 bytes long, got {}",
                data.len()
            )));
        }
        Ok(NaiveDateTime::new(
            date_from_bytes(&data[0..6])?,
            time_from_bytes(&data[6..13])?,
        ))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let year = value.year().to_le_bytes();
        let month = u8::try_from(value.month()).unwrap();
        let day = u8::try_from(value.day()).unwrap();
        let time_since_midnight = value.time().num_seconds_from_midnight().to_le_bytes();
        let nanoseconds = value.time().nanosecond().to_le_bytes();

        [
            year[0],
            year[1],
            year[2],
            year[3],
            month,
            day,
            time_since_midnight[0],
            time_since_midnight[1],
            time_since_midnight[2],
            nanoseconds[0],
            nanoseconds[1],
            nanoseconds[2],
            nanoseconds[3],
        ]
    }

    fn type_name() -> super::TypeName {
        TypeName::new("chrono::NaiveDateTime")
    }
}
impl Key for NaiveDateTime {
    fn compare(data1: &[u8], data2: &[u8]) -> core::cmp::Ordering {
        compare_decoded(data1, data2, |data| NaiveDateTime::try_from_bytes(data))
    }

    fn try_compare(
        data1: &[u8],
        data2: &[u8],
    ) -> core::result::Result<core::cmp::Ordering, ValueError> {
        try_compare_decoded(data1, data2, |data| NaiveDateTime::try_from_bytes(data))
    }
}

impl Value for DateTime<FixedOffset> {
    type SelfType<'a>
        = DateTime<FixedOffset>
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 17]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(17)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        if data.len() != 17 {
            return Err(value_error(format_args!(
                "DateTime<FixedOffset> must be 17 bytes long, got {}",
                data.len()
            )));
        }
        let time = NaiveDateTime::new(
            date_from_bytes(&data[0..6])?,
            time_from_bytes(&data[6..13])?,
        );
        Ok(offset_from_bytes(&data[13..17])?.from_utc_datetime(&time))
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let value_utc = value.with_timezone(&Utc);
        let year = value_utc.year().to_le_bytes();
        let month = u8::try_from(value_utc.month()).unwrap();
        let day = u8::try_from(value_utc.day()).unwrap();
        let time_since_midnight = value_utc.time().num_seconds_from_midnight().to_le_bytes();

        let nanoseconds = value_utc.time().nanosecond().to_le_bytes();

        let offset_seconds = value.offset().local_minus_utc().to_le_bytes();
        [
            year[0],
            year[1],
            year[2],
            year[3],
            month,
            day,
            time_since_midnight[0],
            time_since_midnight[1],
            time_since_midnight[2],
            nanoseconds[0],
            nanoseconds[1],
            nanoseconds[2],
            nanoseconds[3],
            offset_seconds[0],
            offset_seconds[1],
            offset_seconds[2],
            offset_seconds[3],
        ]
    }

    fn type_name() -> TypeName {
        TypeName::new("chrono::DateTime<chrono::FixedOffset>")
    }
}
impl Key for DateTime<FixedOffset> {
    fn compare(data1: &[u8], data2: &[u8]) -> core::cmp::Ordering {
        compare_decoded(data1, data2, |data| {
            DateTime::<FixedOffset>::try_from_bytes(data)
        })
    }

    fn try_compare(
        data1: &[u8],
        data2: &[u8],
    ) -> core::result::Result<core::cmp::Ordering, ValueError> {
        try_compare_decoded(data1, data2, |data| {
            DateTime::<FixedOffset>::try_from_bytes(data)
        })
    }
}
impl Value for FixedOffset {
    type SelfType<'a>
        = FixedOffset
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 4]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(4)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        Self::try_from_bytes(data).unwrap_or_else(|err| panic!("{err}"))
    }

    fn try_from_bytes<'a>(data: &'a [u8]) -> core::result::Result<Self::SelfType<'a>, ValueError>
    where
        Self: 'a,
    {
        if data.len() != 4 {
            return Err(value_error(format_args!(
                "FixedOffset must be 4 bytes long got {}",
                data.len()
            )));
        }
        offset_from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        value.local_minus_utc().to_le_bytes()
    }

    fn type_name() -> TypeName {
        TypeName::new("chrono::FixedOffset")
    }
}
impl Key for FixedOffset {
    fn compare(data1: &[u8], data2: &[u8]) -> core::cmp::Ordering {
        compare_decoded(data1, data2, |data| {
            Ok(FixedOffset::try_from_bytes(data)?.local_minus_utc())
        })
    }

    fn try_compare(
        data1: &[u8],
        data2: &[u8],
    ) -> core::result::Result<core::cmp::Ordering, ValueError> {
        try_compare_decoded(data1, data2, |data| {
            Ok(FixedOffset::try_from_bytes(data)?.local_minus_utc())
        })
    }
}

fn try_compare_decoded<T: Ord>(
    data1: &[u8],
    data2: &[u8],
    decode: impl Fn(&[u8]) -> core::result::Result<T, ValueError>,
) -> core::result::Result<core::cmp::Ordering, ValueError> {
    Ok(decode(data1)?.cmp(&decode(data2)?))
}

fn compare_decoded<T: Ord>(
    data1: &[u8],
    data2: &[u8],
    decode: impl Fn(&[u8]) -> core::result::Result<T, ValueError>,
) -> core::cmp::Ordering {
    try_compare_decoded(data1, data2, decode).unwrap_or_else(|err| panic!("{err}"))
}

fn offset_from_bytes(data: &[u8]) -> core::result::Result<FixedOffset, ValueError> {
    let offset_seconds = i32::from_le_bytes(
        data.try_into()
            .map_err(|_| value_error("invalid chrono::FixedOffset"))?,
    );
    FixedOffset::east_opt(offset_seconds).ok_or_else(|| value_error("invalid chrono::FixedOffset"))
}
fn date_from_bytes(data: &[u8]) -> core::result::Result<NaiveDate, ValueError> {
    if data.len() < 6 {
        return Err(value_error("invalid chrono::NaiveDate"));
    }
    let year = i32::from_le_bytes(data[0..4].try_into().unwrap());
    let month = u32::from(data[4]);
    let day = u32::from(data[5]);
    NaiveDate::from_ymd_opt(year, month, day)
        .ok_or_else(|| value_error("invalid chrono::NaiveDate"))
}
fn time_from_bytes(data: &[u8]) -> core::result::Result<NaiveTime, ValueError> {
    if data.len() < 7 {
        return Err(value_error("invalid chrono::NaiveTime"));
    }
    let num_seconds = u32::from_le_bytes([data[0], data[1], data[2], 0]);
    let num_nanoseconds = u32::from_le_bytes(data[3..7].try_into().unwrap());
    NaiveTime::from_num_seconds_from_midnight_opt(num_seconds, num_nanoseconds)
        .ok_or_else(|| value_error("invalid chrono::NaiveTime"))
}

#[cfg(test)]
mod tests {
    use crate::{Database, Key, ReadableDatabase, TableDefinition, Value};
    use chrono_v0_4::{
        DateTime, FixedOffset, Local, NaiveDate, NaiveDateTime, NaiveTime, TimeZone,
    };
    use tempfile::NamedTempFile;
    const NAIVE_DATE_TABLE: TableDefinition<NaiveDate, i32> =
        TableDefinition::new("naive_date_table");
    const NAIVE_TIME_TABLE: TableDefinition<NaiveTime, i32> =
        TableDefinition::new("naive_time_table");
    const NAIVE_DATETIME_TABLE: TableDefinition<NaiveDateTime, i32> =
        TableDefinition::new("naive_datetime_table");
    const FIXED_OFFSET_TABLE: TableDefinition<FixedOffset, i32> =
        TableDefinition::new("fixed_offset_table");
    const DATETIME_FIXED_OFFSET_TABLE: TableDefinition<DateTime<FixedOffset>, i32> =
        TableDefinition::new("datetime_fixed_offset_table");
    #[test]
    fn test_naive_date() {
        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let bytes = NaiveDate::as_bytes(&date);
        assert_eq!(
            NaiveDate::fixed_width(),
            Some(bytes.len()),
            "NaiveDate should have fixed width"
        );
        assert_eq!(
            NaiveDate::compare(&bytes, &bytes),
            core::cmp::Ordering::Equal,
            "Bytes should compare equal to themselves"
        );
        let date_from_bytes = NaiveDate::from_bytes(&bytes);
        assert_eq!(date, date_from_bytes);
        let mut longer = bytes.to_vec();
        longer.push(0);
        assert!(NaiveDate::try_compare(&longer, &bytes).is_err());
    }
    #[test]
    fn test_naive_time() {
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let bytes = NaiveTime::as_bytes(&time);
        assert_eq!(
            NaiveTime::fixed_width(),
            Some(bytes.len()),
            "NaiveTime should have fixed width"
        );
        assert_eq!(
            NaiveTime::compare(&bytes, &bytes),
            core::cmp::Ordering::Equal,
            "Bytes should compare equal to themselves"
        );
        let time_from_bytes = NaiveTime::from_bytes(&bytes);
        assert_eq!(time, time_from_bytes);
        let mut longer = bytes.to_vec();
        longer.push(0);
        assert!(NaiveTime::try_compare(&longer, &bytes).is_err());
    }
    #[test]
    fn test_naive_date_time() {
        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let datetime = NaiveDateTime::new(date, time);
        let bytes = NaiveDateTime::as_bytes(&datetime);
        assert_eq!(
            NaiveDateTime::fixed_width(),
            Some(bytes.len()),
            "NaiveDateTime should have fixed width"
        );
        assert_eq!(
            NaiveDateTime::compare(&bytes, &bytes),
            core::cmp::Ordering::Equal,
            "Bytes should compare equal to themselves"
        );
        let datetime_from_bytes = NaiveDateTime::from_bytes(&bytes);
        assert_eq!(datetime, datetime_from_bytes);
    }
    #[test]
    fn test_fixed_offset() {
        let offset = FixedOffset::east_opt(3600).unwrap(); // +01:00
        let bytes = FixedOffset::as_bytes(&offset);
        assert_eq!(
            FixedOffset::fixed_width(),
            Some(bytes.len()),
            "FixedOffset should have fixed width"
        );
        assert_eq!(
            FixedOffset::compare(&bytes, &bytes),
            core::cmp::Ordering::Equal,
            "Bytes should compare equal to themselves"
        );
        assert_eq!(
            FixedOffset::try_compare(&bytes, &bytes).unwrap(),
            core::cmp::Ordering::Equal
        );
        // An offset outside -86_400..=86_400 is not a valid encoding.
        let invalid = i32::MAX.to_le_bytes();
        assert!(FixedOffset::try_compare(&invalid, &bytes).is_err());
        let offset_from_bytes = FixedOffset::from_bytes(&bytes);
        assert_eq!(offset, offset_from_bytes);
    }

    #[test]
    #[should_panic(expected = "invalid chrono::FixedOffset")]
    fn malformed_fixed_offset_compare_panics() {
        let bytes = FixedOffset::as_bytes(&FixedOffset::east_opt(3600).unwrap());
        let invalid = i32::MAX.to_le_bytes();
        let _ = FixedOffset::compare(&invalid, &bytes);
    }
    #[test]
    fn test_date_time_fixed_offset() {
        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let naive_datetime = NaiveDateTime::new(date, time);
        for i in -12..=12 {
            let offset = FixedOffset::east_opt(i * 3600).unwrap(); // i hours offset
            let datetime = offset.from_utc_datetime(&naive_datetime);
            let bytes = DateTime::<FixedOffset>::as_bytes(&datetime);
            assert_eq!(
                DateTime::<FixedOffset>::fixed_width(),
                Some(bytes.len()),
                "DateTime<FixedOffset> should have fixed width"
            );
            assert_eq!(
                DateTime::<FixedOffset>::compare(&bytes, &bytes),
                core::cmp::Ordering::Equal,
                "Bytes should compare equal to themselves"
            );
            let datetime_from_bytes = DateTime::<FixedOffset>::from_bytes(&bytes);
            assert_eq!(datetime, datetime_from_bytes);
        }
    }
    #[test]
    fn test_fixed_offset_table() {
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(FIXED_OFFSET_TABLE).unwrap();
            let offset = FixedOffset::east_opt(3600).unwrap(); // +01:00
            table.insert(offset, 1).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(FIXED_OFFSET_TABLE).unwrap();
            let offset = FixedOffset::east_opt(3600).unwrap(); // +01:00
            let value = table.get_owned(&offset).unwrap().unwrap();
            assert_eq!(value.value(), 1);
        }
    }
    #[test]
    fn test_naive_date_table() {
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(NAIVE_DATE_TABLE).unwrap();
            let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
            table.insert(date, 1).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(NAIVE_DATE_TABLE).unwrap();
            let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
            let value = table.get_owned(&date).unwrap().unwrap();
            assert_eq!(value.value(), 1);
        }
    }
    #[test]
    fn test_naive_time_table() {
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(NAIVE_TIME_TABLE).unwrap();
            let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
            table.insert(time, 1).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(NAIVE_TIME_TABLE).unwrap();
            let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
            let value = table.get_owned(&time).unwrap().unwrap();
            assert_eq!(value.value(), 1);
        }
    }

    #[test]
    fn test_naive_datetime_table() {
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(NAIVE_DATETIME_TABLE).unwrap();
            let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
            let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
            let datetime = NaiveDateTime::new(date, time);
            table.insert(datetime, 1).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(NAIVE_DATETIME_TABLE).unwrap();
            let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
            let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
            let datetime = NaiveDateTime::new(date, time);
            let value = table.get_owned(&datetime).unwrap().unwrap();
            assert_eq!(value.value(), 1);
        }
    }
    #[test]
    fn test_datetime_fixed_offset_table() {
        let now = Local::now().fixed_offset();
        let db = Database::create(NamedTempFile::new().unwrap()).unwrap();
        let write_txn = db.begin_write().unwrap();
        {
            let mut table = write_txn.open_table(DATETIME_FIXED_OFFSET_TABLE).unwrap();
            table.insert(now, 1).unwrap();
        }
        write_txn.commit().unwrap();

        let read_txn = db.begin_read().unwrap();
        {
            let table = read_txn.open_table(DATETIME_FIXED_OFFSET_TABLE).unwrap();
            let value = table.get_owned(&now).unwrap().unwrap();
            assert_eq!(value.value(), 1);
        }
    }
}
