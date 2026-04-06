use std::path::{Path, PathBuf};

const SECONDS_PER_DAY: i64 = 86_400;

fn div_floor(value: i64, divisor: i64) -> i64 {
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder < 0 {
        quotient - 1
    } else {
        quotient
    }
}

fn utc_components(timestamp: i64) -> (i32, u32, u32, u32, u32, u32) {
    let days = div_floor(timestamp, SECONDS_PER_DAY);
    let seconds_of_day = timestamp - days * SECONDS_PER_DAY;
    let hour = (seconds_of_day / 3_600) as u32;
    let minute = ((seconds_of_day % 3_600) / 60) as u32;
    let second = (seconds_of_day % 60) as u32;

    let shifted_days = days + 719_468;
    let era = if shifted_days >= 0 {
        shifted_days
    } else {
        shifted_days - 146_096
    } / 146_097;
    let day_of_era = shifted_days - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    let year = year_of_era + era * 400 + if month <= 2 { 1 } else { 0 };

    (year as i32, month as u32, day as u32, hour, minute, second)
}

pub fn format_date(timestamp: i64) -> String {
    let (year, month, day, _, _, _) = utc_components(timestamp);
    format!("{year:04}-{month:02}-{day:02}")
}

pub fn format_compact_timestamp(timestamp: i64) -> String {
    let (year, month, day, hour, minute, second) = utc_components(timestamp);
    format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z")
}

pub fn partition_directory(root: &Path, timestamp_min: i64, timestamp_max: i64) -> PathBuf {
    root.join(format!("date_start={}", format_date(timestamp_min)))
        .join(format!("date_end={}", format_date(timestamp_max)))
}

pub fn batch_file_name(
    batch_start_id: u64,
    timestamp_min: i64,
    timestamp_max: i64,
    extension: &str,
) -> String {
    format!(
        "batch_{batch_start_id:010}_{}_{}.{}",
        format_compact_timestamp(timestamp_min),
        format_compact_timestamp(timestamp_max),
        extension
    )
}

pub fn find_batch_file(root: &Path, batch_start_id: u64, extension: &str) -> Option<PathBuf> {
    if !root.exists() {
        return None;
    }

    let exact_name = format!("batch_{batch_start_id:010}.{extension}");
    let prefixed_name = format!("batch_{batch_start_id:010}_");
    let expected_suffix = format!(".{extension}");
    let mut pending = vec![root.to_path_buf()];

    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir).ok()?;
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }

            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };

            if file_name == exact_name
                || (file_name.starts_with(&prefixed_name) && file_name.ends_with(&expected_suffix))
            {
                return Some(path);
            }
        }
    }

    None
}
