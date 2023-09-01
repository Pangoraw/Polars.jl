use crate::{series::make_series, *};

#[repr(C)]
pub enum polars_value_type_t {
    PolarsValueTypeNull,
    PolarsValueTypeBoolean,
    PolarsValueTypeUInt8,
    PolarsValueTypeUInt16,
    PolarsValueTypeUInt32,
    PolarsValueTypeUInt64,
    PolarsValueTypeInt8,
    PolarsValueTypeInt16,
    PolarsValueTypeInt32,
    PolarsValueTypeInt64,
    PolarsValueTypeFloat32,
    PolarsValueTypeFloat64,
    PolarsValueTypeList,
    PolarsValueTypeUnknown,
}

impl polars_value_type_t {
    pub(crate) fn from_dtype(d: &DataType) -> Self {
        use polars_value_type_t::*;
        match d {
            DataType::Null => PolarsValueTypeNull,
            DataType::Boolean => PolarsValueTypeBoolean,
            DataType::UInt8 => PolarsValueTypeUInt8,
            DataType::UInt16 => PolarsValueTypeUInt16,
            DataType::UInt32 => PolarsValueTypeUInt32,
            DataType::UInt64 => PolarsValueTypeUInt64,
            DataType::Int8 => PolarsValueTypeInt8,
            DataType::Int16 => PolarsValueTypeInt16,
            DataType::Int32 => PolarsValueTypeInt32,
            DataType::Int64 => PolarsValueTypeInt64,
            DataType::Float32 => PolarsValueTypeFloat32,
            DataType::Float64 => PolarsValueTypeFloat64,
            DataType::List(_) => PolarsValueTypeList,
            _ => PolarsValueTypeUnknown,
        }
    }
}

#[no_mangle]
pub extern "C" fn polars_value_type(value: *mut polars_value_t) -> polars_value_type_t {
    polars_value_type_t::from_dtype(unsafe { &(*value).inner.dtype() })
}

#[no_mangle]
pub unsafe extern "C" fn polars_value_destroy(value: *mut polars_value_t) {
    assert!(!value.is_null());
    let _ = Box::from_raw(value);
}

macro_rules! gen_value_get {
    ($n: ident, $t: ident, $rt: ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(
            value: *mut polars_value_t,
            out: *mut $t,
        ) -> *const polars_error_t {
            match (*value).inner {
                AnyValue::$rt(value) => *out = value,
                _ => return make_error(concat!("value is not of type ", stringify!($rt))),
            }
            std::ptr::null()
        }
    };
}

gen_value_get!(polars_value_get_bool, bool, Boolean);
gen_value_get!(polars_value_get_u8, u8, UInt8);
gen_value_get!(polars_value_get_u16, u16, UInt16);
gen_value_get!(polars_value_get_u32, u32, UInt32);
gen_value_get!(polars_value_get_u64, u64, UInt64);
gen_value_get!(polars_value_get_i8, i8, Int8);
gen_value_get!(polars_value_get_i16, i16, Int16);
gen_value_get!(polars_value_get_i32, i32, Int32);
gen_value_get!(polars_value_get_i64, i64, Int64);
gen_value_get!(polars_value_get_f32, f32, Float32);
gen_value_get!(polars_value_get_f64, f64, Float64);

/// Returns the value as a Series when the dtype of the value is a list.
#[no_mangle]
pub unsafe extern "C" fn polars_value_list_get(
    value: *mut polars_value_t,
    out: *mut *mut polars_series_t,
) -> *const polars_error_t {
    match &(*value).inner {
        AnyValue::List(series) => *out = make_series(series.clone()),
        _ => return make_error("value is not of type list"),
    }
    std::ptr::null()
}

/// Returns the element type of the provided value which must be a list.
/// The value type is PolarsValueTypeUnknown if the value is not a list
/// so makes sure it is one otherwise, you cannot differentiate between list<unkown>
/// and unkown.
#[no_mangle]
pub unsafe extern "C" fn polars_value_list_type(value: *mut polars_value_t) -> polars_value_type_t {
    match (*value).inner.dtype() {
        DataType::List(eltype) => polars_value_type_t::from_dtype(&eltype),
        _ => polars_value_type_t::PolarsValueTypeUnknown,
    }
}
