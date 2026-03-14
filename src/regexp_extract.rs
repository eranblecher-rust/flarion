use crate::regexp_kernel::regexp_extract;
use datafusion::arrow::array::StringBuilder;
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion::error::{Result, DataFusionError};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::utils::{get_i64_val, get_str_val};

const EXPECTED_ARGS: usize = 3;
const STR_COL_IDX: usize = 0;
const PAT_COL_IDX: usize = 1;
const IDX_COL_IDX: usize = 2;

const DEFAULT_BUILDER_CAPACITY: usize = 1024;

pub struct RegexpExtractUDF {
    signature: Signature,
}

impl RegexpExtractUDF {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Int64],
                Volatility::Immutable,
            ),
        }
    }
}

impl Debug for RegexpExtractUDF {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RegexpExtractUDF")
    }
}

impl ScalarUDFImpl for RegexpExtractUDF {
    fn as_any(&self) -> &dyn Any { self }

    fn name(&self) -> &str { "regexp_extract" }

    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_batch(&self, args: &[ColumnarValue], batch_size: usize) -> Result<ColumnarValue> {
        if args.len() != EXPECTED_ARGS {
            return Err(DataFusionError::Internal(format!(
                "regexp_extract expects {} arguments, got {}",
                EXPECTED_ARGS,
                args.len()
            )));
        }

        let mut builder = StringBuilder::with_capacity(batch_size, DEFAULT_BUILDER_CAPACITY);

        for i in 0..batch_size {
            let s = get_str_val(&args[STR_COL_IDX], i);
            let p = get_str_val(&args[PAT_COL_IDX], i);
            let idx = get_i64_val(&args[IDX_COL_IDX], i);

            match (s, p, idx) {
                (Some(input), Some(pattern), Some(i)) if i >= 0 => {
                    match regexp_extract(input, pattern, i) {
                        Some(val) => builder.append_value(val),
                        None => builder.append_null(),
                    }
                }
                // אם אחד הארגומנטים הוא NULL או האינדקס שלילי
                _ => builder.append_null(),
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Array;
    use super::*;
    use datafusion::arrow::array::{StringArray, Int64Array, ArrayRef};
    use datafusion::logical_expr::ColumnarValue;

    #[test]
    fn test_regexp_extract_basic() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("email: eran@gmail.com"), None]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\w+)@(\w+\.\w+)", r"(\d+)"]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![2, 1]));

        let args = vec![
            ColumnarValue::Array(str_arr),
            ColumnarValue::Array(pat_arr),
            ColumnarValue::Array(idx_arr)
        ];

        let result = udf.invoke_batch(&args, 2)?;
        let arr = result.into_array(2)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "gmail.com");
        assert!(res_arr.is_null(1));
        Ok(())
    }

    #[test]
    fn test_edge_cases() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("data123"), Some("data123")]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\d+)", r"[invalid"]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![99, 1]));

        let args = vec![
            ColumnarValue::Array(str_arr),
            ColumnarValue::Array(pat_arr),
            ColumnarValue::Array(idx_arr)
        ];

        let result = udf.invoke_batch(&args, 2)?;
        let arr = result.into_array(2)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "");
        assert_eq!(res_arr.value(1), "");
        Ok(())
    }

    #[test]
    fn test_large_batch_processing() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let batch_size = 1000;

        let strings: Vec<Option<&str>> = vec![Some("test12345"); batch_size];
        let patterns: Vec<Option<&str>> = vec![Some(r"(\d+)"); batch_size];
        let indices: Vec<i64> = vec![1; batch_size];

        let str_arr: ArrayRef = Arc::new(StringArray::from(strings));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(patterns));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(indices));

        let args = vec![
            ColumnarValue::Array(str_arr),
            ColumnarValue::Array(pat_arr),
            ColumnarValue::Array(idx_arr),
        ];

        let result = udf.invoke_batch(&args, batch_size)?;
        let arr = result.into_array(batch_size)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.len(), batch_size);
        let val = match res_arr.iter().nth(500).flatten() {
            Some(v) => v,
            None => panic!("Index 500 should exist"),
        };

        assert_eq!(val, "12345");

        Ok(())
    }

    #[test]
    fn test_regexp_extract_with_scalar_pattern() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let batch_size = 3;
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![
            Some("user_123"),
            Some("user_456"),
            Some("user_789")
        ]));

        let pat_scalar = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(r"(\d+)".to_string())));

        let idx_scalar = ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Int64(Some(1)));

        let args = vec![
            ColumnarValue::Array(str_arr),
            pat_scalar,
            idx_scalar
        ];

        let result = udf.invoke_batch(&args, batch_size)?;
        let arr = result.into_array(batch_size)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "123");
        assert_eq!(res_arr.value(1), "456");
        assert_eq!(res_arr.value(2), "789");

        Ok(())
    }

    #[test]
    fn test_negative_index_handling() {
        let udf = RegexpExtractUDF::new();
        let batch_size = 1;

        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("email: test@gmail.com")]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\w+)@(\w+\.\w+)"]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![-1]));

        let args = vec![
            ColumnarValue::Array(str_arr),
            ColumnarValue::Array(pat_arr),
            ColumnarValue::Array(idx_arr)
        ];

        let result = udf.invoke_batch(&args, batch_size).unwrap();
        let arr = result.into_array(batch_size).unwrap();
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert!(res_arr.is_null(0), "Negative index should result in a NULL value");
    }

    #[test]
    fn test_out_of_bound_index_handling() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("email: test@gmail.com")]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\w+)@(\w+\.\w+)"]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![100]));

        let args = vec![ColumnarValue::Array(str_arr), ColumnarValue::Array(pat_arr), ColumnarValue::Array(idx_arr)];
        let result = udf.invoke_batch(&args, 1)?;
        let arr = result.into_array(1)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "");
        Ok(())
    }

    #[test]
    fn test_unicode_support() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("שלום: 12345")]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\d+)"]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![1]));

        let args = vec![ColumnarValue::Array(str_arr), ColumnarValue::Array(pat_arr), ColumnarValue::Array(idx_arr)];
        let result = udf.invoke_batch(&args, 1)?;
        let arr = result.into_array(1)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "12345");
        Ok(())
    }

    #[test]
    fn test_empty_batch() -> Result<()> {
        let udf = RegexpExtractUDF::new();
        let str_arr: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(Vec::<Option<&str>>::new()));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));

        let args = vec![ColumnarValue::Array(str_arr), ColumnarValue::Array(pat_arr), ColumnarValue::Array(idx_arr)];
        let result = udf.invoke_batch(&args, 0)?;
        let arr = result.into_array(0)?;
        assert_eq!(arr.len(), 0);
        Ok(())
    }

    #[test]
    fn test_mixed_nulls() -> Result<()> {
        let udf = RegexpExtractUDF::new();

        let str_arr: ArrayRef = Arc::new(StringArray::from(vec![Some("a@b.com"), None, Some("c@d.com")]));
        let pat_arr: ArrayRef = Arc::new(StringArray::from(vec![r"(\w+)@(\w+\.\w+)"; 3]));
        let idx_arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 1]));

        let args = vec![ColumnarValue::Array(str_arr), ColumnarValue::Array(pat_arr), ColumnarValue::Array(idx_arr)];
        let result = udf.invoke_batch(&args, 3)?;
        let arr = result.into_array(3)?;
        let res_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(res_arr.value(0), "a");
        assert!(res_arr.is_null(1));
        assert_eq!(res_arr.value(2), "c");
        Ok(())
    }
}
