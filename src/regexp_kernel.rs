use regex::Regex;
use moka::sync::Cache;
use std::sync::{OnceLock, Arc};
use std::time::Duration;
use rustc_hash::FxHasher;
use std::hash::Hasher;

type CacheValue = Arc<Vec<Option<String>>>;

static REGEX_COMPILE_CACHE: OnceLock<Cache<String, Arc<Regex>>> = OnceLock::new();
static REGEX_RESULT_CACHE: OnceLock<Cache<u64, CacheValue>> = OnceLock::new();

// --- Caching Thresholds ---
const CACHE_MIN_INPUT_LEN: usize = 16;
const CACHE_MIN_PATTERN_LEN: usize = 8;

// --- Cache Configuration ---
const REGEX_COMPILE_CACHE_CAPACITY: u64 = 1_000;
const REGEX_RESULT_CACHE_CAPACITY: u64 = 10_000;
const REGEX_RESULT_CACHE_IDLE_TIMEOUT: Duration = Duration::from_secs(3600);

fn hash_key(input: &str, pattern: &str) -> u64 {
    let mut hasher = FxHasher::default();
    hasher.write(input.as_bytes());
    hasher.write(pattern.as_bytes());
    hasher.finish()
}

fn compiled_regex(pattern: &str) -> Option<Arc<Regex>> {
    let cache = REGEX_COMPILE_CACHE.get_or_init(|| {
        Cache::builder().max_capacity(REGEX_COMPILE_CACHE_CAPACITY).build()
    });
    if let Some(regex) = cache.get(pattern) { return Some(regex); }
    let regex = Arc::new(Regex::new(pattern).ok()?);
    cache.insert(pattern.to_string(), regex.clone());
    Some(regex)
}

fn perform_regex_extraction(input: &str, pattern: &str) -> CacheValue {
    let regex = match compiled_regex(pattern) {
        Some(r) => r,
        None => return Arc::new(Vec::new()),
    };
    let result: Vec<Option<String>> = regex.captures(input)
        .map(|caps| caps.iter().map(|m| m.map(|mat| mat.as_str().to_string())).collect())
        .unwrap_or_default();
    Arc::new(result)
}

pub fn regexp_extract(input: &str, pattern: &str, idx: i64) -> Option<String> {
    if idx < 0 { return None; }
    if input.len() < CACHE_MIN_INPUT_LEN || pattern.len() < CACHE_MIN_PATTERN_LEN {
        return perform_regex_extraction(input, pattern).get(idx as usize).and_then(|opt| opt.clone());
    }
    let cache = REGEX_RESULT_CACHE.get_or_init(|| {
        Cache::builder()
            .max_capacity(REGEX_RESULT_CACHE_CAPACITY)
            .time_to_idle(REGEX_RESULT_CACHE_IDLE_TIMEOUT)
            .build()
    });
    let key = hash_key(input, pattern);
    if let Some(caps) = cache.get(&key) { return caps.get(idx as usize).and_then(|opt| opt.clone()); }
    let all_caps = perform_regex_extraction(input, pattern);
    cache.insert(key, all_caps.clone());
    all_caps.get(idx as usize).and_then(|opt| opt.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn basic_extraction() {
        let input = "email: test@example.com";
        let pattern = r"(\w+)@(\w+\.\w+)";
        assert_eq!(regexp_extract(input, pattern, 1), Some("test".to_string()));
    }

    #[test]
    fn test_regex_compile_cache() {
        let pattern = r"(\d{4})-(\d{2})-(\d{2})";
        let re1 = compiled_regex(pattern).expect("Failed to compile regex");
        let re2 = compiled_regex(pattern).expect("Failed to compile regex");
        assert!(Arc::ptr_eq(&re1, &re2));
    }

    #[test]
    fn test_regex_compile_cache_performance() {
        let pattern = "a".repeat(50) + &r"(\d{4})-(\d{2})-(\d{2})";
        let iterations = 100;
        let _ = compiled_regex(&pattern).unwrap();
        let start_first = Instant::now();
        for _ in 0..iterations { let _ = Regex::new(&pattern).unwrap(); }
        let duration_first = start_first.elapsed();
        let start_second = Instant::now();
        for _ in 0..iterations { let _ = compiled_regex(&pattern).unwrap(); }
        let duration_second = start_second.elapsed();
        assert!(duration_second < duration_first / 50);
    }

    #[test]
    fn test_cache_performance_impact() {
        let input = "a".repeat(1000) + "2026-03-12" + &"a".repeat(1000);
        let pattern = r"(\d{4})-(\d{2})-(\d{2})";
        let start_first = Instant::now();
        let _ = regexp_extract(&input, pattern, 1);
        let duration_first = start_first.elapsed();
        let start_second = Instant::now();
        let _ = regexp_extract(&input, pattern, 2);
        let duration_second = start_second.elapsed();
        assert!(duration_second < duration_first);
    }

    #[test]
    fn invalid_pattern() {
        let input = "test";
        let pattern = r"([a-z";
        assert_eq!(regexp_extract(input, pattern, 1), None);
    }

    #[test]
    fn test_negative_index_in_kernel() {
        let input = "email: test@example.com";
        let pattern = r"(\w+)@(\w+\.\w+)";
        let result = regexp_extract(input, pattern, -1);
        assert!(result.is_none());
    }
}
