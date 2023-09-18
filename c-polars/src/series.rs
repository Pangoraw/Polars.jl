use crate::{value::polars_value_type_t, *};

pub(crate) fn make_series(series: Series) -> *mut polars_series_t {
    Box::into_raw(Box::new(polars_series_t { inner: series }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_destroy(series: *mut polars_series_t) {
    assert!(!series.is_null());
    let _ = Box::from_raw(series);
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_type(series: *mut polars_series_t) -> polars_value_type_t {
    polars_value_type_t::from_dtype((*series).inner.dtype())
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_length(series: *mut polars_series_t) -> usize {
    assert!(!series.is_null());
    (*series).inner.len()
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_null_count(series: *mut polars_series_t) -> usize {
    assert!(!series.is_null());
    (*series).inner.null_count()
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_schema(series: *mut polars_series_t) -> ArrowSchema {
    assert!(!series.is_null());
    ffi::export_field_to_c(&(*series).inner.field().to_arrow())
}

/// Returns whether or not the value at index `index` is null, return false if the index is out of
/// bounds.
#[no_mangle]
pub unsafe extern "C" fn polars_series_is_null(series: *mut polars_series_t, index: usize) -> bool {
    assert!(!series.is_null());
    match (*series).inner.get(index) {
        Ok(AnyValue::Null) => true,
        Ok(_) => false,
        Err(_) => false,
    }
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_name(
    series: *mut polars_series_t,
    out: *mut *const u8,
) -> usize {
    assert!(!series.is_null());
    let name = (*series).inner.name();
    *out = name.as_ptr();
    name.len()
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_get<'a>(
    series: *mut polars_series_t,
    index: usize,
) -> *const polars_value_t<'a> {
    assert!(!series.is_null());
    let value = (*series).inner.get(index).unwrap();
    Box::into_raw(Box::new(polars_value_t { inner: value }))
}

macro_rules! gen_series_get {
    ($n: ident, $t: ident, $rt: ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(
            series: *mut polars_series_t,
            index: usize,
            out: *mut $t,
        ) -> *const polars_error_t {
            assert!(!series.is_null());
            match (*series).inner.get(index) {
                Ok(AnyValue::$rt(value)) => {
                    *out = value;
                    std::ptr::null()
                }
                Ok(_) => make_error("series type is invalid"),
                Err(err) => make_error(err),
            }
        }
    };
}

gen_series_get!(polars_series_get_bool, bool, Boolean);
gen_series_get!(polars_series_get_u8, u8, UInt8);
gen_series_get!(polars_series_get_u16, u16, UInt16);
gen_series_get!(polars_series_get_u32, u32, UInt32);
gen_series_get!(polars_series_get_u64, u64, UInt64);
gen_series_get!(polars_series_get_i8, i8, Int8);
gen_series_get!(polars_series_get_i16, i16, Int16);
gen_series_get!(polars_series_get_i32, i32, Int32);
gen_series_get!(polars_series_get_i64, i64, Int64);
gen_series_get!(polars_series_get_f32, f32, Float32);
gen_series_get!(polars_series_get_f64, f64, Float64);
