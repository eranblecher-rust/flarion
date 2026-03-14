use regex::Regex;
use moka::sync::Cache;
use std::sync::{OnceLock, Arc};

static REGEX_COMPILE_CACHE: OnceLock<Cache<String, Arc<Regex>>> = OnceLock::new();

// --- Cache Configuration ---
const REGEX_COMPILE_CACHE_CAPACITY: u64 = 1_000;

fn compiled_regex(pattern: &str) -> Option<Arc<Regex>> {
    let cache = REGEX_COMPILE_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(REGEX_COMPILE_CACHE_CAPACITY)
            .build()
    });

    if let Some(regex) = cache.get(pattern) {
        return Some(regex);
    }

    let regex = Arc::new(Regex::new(pattern).ok()?);
    cache.insert(pattern.to_string(), regex.clone());
    Some(regex)
}

pub fn regexp_extract(input: &str, pattern: &str, idx: i64) -> Option<String> {
    if idx < 0 {
        return None;
    }

    let regex = compiled_regex(pattern)?;
    let captures = regex.captures(input);

    match captures {
        Some(caps) => {
            let idx = idx as _;
            if idx >= caps.len() {
                Some("".to_string())
            } else {
                Some(caps.get(idx).map(|m| m.as_str()).unwrap_or("").to_string())
            }
        }
        None => {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_extraction() {
        let input = "email: eran@gmail.com";
        let pattern = r"(\w+)@(\w+\.\w+)";
        assert_eq!(regexp_extract(input, pattern, 1), Some("eran".to_string()));
        assert_eq!(regexp_extract(input, pattern, 2), Some("gmail.com".to_string()));
    }

    #[test]
    fn test_no_match() {
        assert_eq!(regexp_extract("hello world", r"\d+", 0), None);
    }

    #[test]
    fn test_spark_behavior_out_of_bounds() {
        let input = "data123";
        let pattern = r"(\d+)";
        assert_eq!(regexp_extract(input, pattern, 100), Some("".to_string()));
    }

    #[test]
    fn test_regex_compile_cache() {
        let pattern = r"(\d{4})-(\d{2})-(\d{2})";
        let re1 = compiled_regex(pattern).expect("Failed to compile");
        let re2 = compiled_regex(pattern).expect("Failed to compile");

        assert!(Arc::ptr_eq(&re1, &re2));
    }

    #[test]
    fn test_invalid_pattern() {
        let input = "test";
        let pattern = r"([a-z";
        assert_eq!(regexp_extract(input, pattern, 1), None);
    }

    #[test]
    fn test_negative_index() {
        let input = "test123";
        let pattern = r"(\d+)";
        assert_eq!(regexp_extract(input, pattern, -1), None);
    }

    #[test]
    fn test_optional_group_not_found() {
        let input = "abc";
        let pattern = r"(a)(b)?(c)(d)?";

        assert_eq!(regexp_extract(input, pattern, 4), Some("".to_string()));
    }
}