use arrow::array::Array;
use datafusion::arrow::array::{StringArray, LargeStringArray, Int64Array};
use datafusion::logical_expr::ColumnarValue;
use datafusion::scalar::ScalarValue;

pub fn get_str_val(col: &ColumnarValue, i: usize) -> Option<&str> {
    match col {
        ColumnarValue::Array(a) => {
            if let Some(arr) = a.as_any().downcast_ref::<StringArray>() {
                return if arr.is_null(i) { None } else { Some(arr.value(i)) };
            }
            if let Some(arr) = a.as_any().downcast_ref::<LargeStringArray>() {
                return if arr.is_null(i) { None } else { Some(arr.value(i)) };
            }
            None
        }
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(v))) => Some(v),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(v))) => Some(v),
        _ => None,
        _ => None,
    }
}

pub fn get_i64_val<'a>(col: &ColumnarValue, i: usize) -> Option<i64> {
    match col {
        ColumnarValue::Array(a) => {
            let arr = a.as_any().downcast_ref::<Int64Array>()?;
            if arr.is_null(i) { None } else { Some(arr.value(i)) }
        }
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Some(*v),
        _ => None,
    }
}