use crate::{TypeName, Value};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
macro_rules! thirty_two_bit_bytes {
    (
        $($item:ident),*
    ) => {
        [
            $(
                $item[0], $item[1], $item[2], $item[3],
            )*
        ]
    };
}
impl Value for NaiveDate {
    type SelfType<'a>
        = NaiveDate
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 12]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(12)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.len() < 12 {
            panic!("Not enough bytes to create NaiveDate");
        }
        date_from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let year = value.year().to_le_bytes();
        let month = value.month().to_le_bytes();
        let day = value.day().to_le_bytes();
        thirty_two_bit_bytes![year, month, day]
    }

    fn type_name() -> super::TypeName {
        TypeName::internal("NaiveDate")
    }
}
impl Value for NaiveTime {
    type SelfType<'a>
        = NaiveTime
    where
        Self: 'a;
    type AsBytes<'a>
        = [u8; 8]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(8)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.len() != 8 {
            panic!(
                "Invalid byte length for NaiveTime, expected 8 bytes, got {}",
                data.len()
            );
        }
        time_from_bytes(data)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let num_seconds = value.num_seconds_from_midnight().to_le_bytes();
        let num_nanoseconds = value.nanosecond().to_le_bytes();
        thirty_two_bit_bytes![num_seconds, num_nanoseconds]
    }

    fn type_name() -> TypeName {
        TypeName::internal("NaiveTime")
    }
}
impl Value for NaiveDateTime {
    type SelfType<'a>
        = NaiveDateTime
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 20]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(20)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.len() != 20 {
            panic!(
                "Invalid byte length for NaiveDateTime, expected 20 bytes, got {}",
                data.len()
            );
        }

        let date = date_from_bytes(&data[0..12]);
        let time = time_from_bytes(&data[12..20]);
        NaiveDateTime::new(date, time)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let year = value.year().to_le_bytes();
        let month = value.month().to_le_bytes();
        let day = value.day().to_le_bytes();
        let time_since_midnight = value.time().num_seconds_from_midnight().to_le_bytes();
        let nanoseconds = value.time().nanosecond().to_le_bytes();

        thirty_two_bit_bytes![year, month, day, time_since_midnight, nanoseconds]
    }

    fn type_name() -> super::TypeName {
        TypeName::internal("NaiveDate")
    }
}
impl Value for DateTime<FixedOffset> {
    type SelfType<'a>
        = DateTime<FixedOffset>
    where
        Self: 'a;

    type AsBytes<'a>
        = [u8; 24]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(24)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        if data.len() != 24 {
            panic!(
                "Invalid byte length for DateTime<FixedOffset>, expected 24 bytes, got {}",
                data.len()
            );
        }
        let date = date_from_bytes(&data[0..12]);
        let time = time_from_bytes(&data[12..20]);

        let offset_seconds = i32::from_le_bytes(data[20..24].try_into().unwrap());
        let offset = FixedOffset::east_opt(offset_seconds)
            .expect("Invalid offset seconds, must be between -86400 and 86400");
        let time = NaiveDateTime::new(date, time);

        DateTime::from_naive_utc_and_offset(time, offset)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        let year = value.year().to_le_bytes();
        let month = value.month().to_le_bytes();
        let day = value.day().to_le_bytes();
        let time_since_midnight = value.time().num_seconds_from_midnight().to_le_bytes();
        let nanoseconds = value.time().nanosecond().to_le_bytes();

        let offset_seconds = value.offset().local_minus_utc().to_le_bytes();

        thirty_two_bit_bytes![
            year,
            month,
            day,
            time_since_midnight,
            nanoseconds,
            offset_seconds
        ]
    }

    fn type_name() -> TypeName {
        todo!()
    }
}

fn date_from_bytes(data: &[u8]) -> NaiveDate {
    let year = i32::from_le_bytes(data[0..4].try_into().unwrap());
    let month = u32::from_le_bytes(data[4..8].try_into().unwrap());
    let day = u32::from_le_bytes(data[8..12].try_into().unwrap());
    NaiveDate::from_ymd_opt(year, month, day).expect("Invalid date")
}
fn time_from_bytes(data: &[u8]) -> NaiveTime {
    let num_seconds = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let num_nanoseconds = u32::from_le_bytes(data[4..8].try_into().unwrap());
    NaiveTime::from_num_seconds_from_midnight_opt(num_seconds, num_nanoseconds)
        .expect("Invalid time")
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveTime};

    #[test]
    fn test_naive_date() {
        use crate::Value;
        use chrono::NaiveDate;

        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let bytes = NaiveDate::as_bytes(&date);
        let date_from_bytes = NaiveDate::from_bytes(&bytes);
        assert_eq!(date, date_from_bytes);
    }
    #[test]
    fn test_naive_time() {
        use crate::Value;
        use chrono::NaiveTime;

        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let bytes = NaiveTime::as_bytes(&time);
        let time_from_bytes = NaiveTime::from_bytes(&bytes);
        assert_eq!(time, time_from_bytes);
    }
    #[test]
    fn test_naive_date_time() {
        use crate::Value;
        use chrono::NaiveDateTime;

        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let datetime = NaiveDateTime::new(date, time);
        let bytes = NaiveDateTime::as_bytes(&datetime);
        let datetime_from_bytes = NaiveDateTime::from_bytes(&bytes);
        assert_eq!(datetime, datetime_from_bytes);
    }

    #[test]
    fn test_date_time_fixed_offset() {
        use crate::Value;
        use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};

        let date = NaiveDate::from_ymd_opt(2023, 10, 5).unwrap();
        let time = NaiveTime::from_hms_opt(12, 30, 45).unwrap();
        let naive_datetime = NaiveDateTime::new(date, time);
        let offset = FixedOffset::east_opt(3600).unwrap(); // +01:00
        let datetime = DateTime::from_naive_utc_and_offset(naive_datetime, offset);
        let bytes = DateTime::<FixedOffset>::as_bytes(&datetime);
        let datetime_from_bytes = DateTime::<FixedOffset>::from_bytes(&bytes);
        assert_eq!(datetime, datetime_from_bytes);
    }
}
