use polars_core::utils::arrow::array::Int64Array;

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
    PolarsValueTypeUtf8,
    PolarsValueTypeStruct,
    PolarsValueTypeBinary,
    PolarsValueTypeCategorical,
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
            DataType::Utf8 => PolarsValueTypeUtf8,
            DataType::Struct(_) => PolarsValueTypeStruct,
            DataType::Binary => PolarsValueTypeBinary,
            DataType::Categorical(_) => PolarsValueTypeCategorical,
            DataType::Unknown => PolarsValueTypeUnknown,
            _ => PolarsValueTypeUnknown,
        }
    }

    pub(crate) fn to_dtype(&self) -> DataType {
        use polars_value_type_t::*;
        match self {
            PolarsValueTypeNull => DataType::Null,
            PolarsValueTypeBoolean => DataType::Boolean,
            PolarsValueTypeUInt8 => DataType::UInt8,
            PolarsValueTypeUInt16 => DataType::UInt16,
            PolarsValueTypeUInt32 => DataType::UInt32,
            PolarsValueTypeUInt64 => DataType::UInt64,
            PolarsValueTypeInt8 => DataType::Int8,
            PolarsValueTypeInt16 => DataType::Int16,
            PolarsValueTypeInt32 => DataType::Int32,
            PolarsValueTypeInt64 => DataType::Int64,
            PolarsValueTypeFloat32 => DataType::Float32,
            PolarsValueTypeFloat64 => DataType::Float64,
            PolarsValueTypeUtf8 => DataType::Utf8,
            PolarsValueTypeBinary => DataType::Binary,
            PolarsValueTypeUnknown => DataType::Unknown,
            _ => DataType::Unknown, // Cannot map structs and lists
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

#[no_mangle]
pub unsafe extern "C" fn polars_value_utf8_get(
    value: *mut polars_value_t,
    user: *mut c_void,
    callback: IOCallback,
) -> *const polars_error_t {
    let mut w = UserIOCallback(callback, user);
    let Err(err) = (match (*value).inner {
        AnyValue::Utf8(s) => w.write(s.as_bytes()),
        AnyValue::Categorical(_, _, _) => {
            return polars_value_categorical_get(value, user, callback)
        }
        _ => return make_error("value is not of type utf8"),
    }) else {
        return std::ptr::null();
    };
    make_error(err)
}

#[no_mangle]
pub unsafe extern "C" fn polars_value_binary_get(
    value: *mut polars_value_t,
    user: *mut c_void,
    callback: IOCallback,
) -> *const polars_error_t {
    let mut w = UserIOCallback(callback, user);
    let Err(err) = (match (*value).inner {
        AnyValue::Binary(s) => w.write(s),
        _ => return make_error("value is not of type utf8"),
    }) else {
        return std::ptr::null();
    };
    make_error(err)
}

#[no_mangle]
pub unsafe extern "C" fn polars_value_categorical_get(
    value: *mut polars_value_t,
    user: *mut c_void,
    callback: IOCallback,
) -> *const polars_error_t {
    let AnyValue::Categorical(idx, rev_mapping, pool) = &(*value).inner else {
        return make_error("invalid type for value");
    };

    let v = if pool.is_null() {
        rev_mapping.get(*idx)
    } else {
        let p = pool.get();
        let Some(v) = (*p).get(*idx as usize) else {
            return make_error("invalid");
        };
        v
    };

    let mut w = UserIOCallback(callback, user);
    let Ok(_) = w.write(v.as_bytes()) else {
        return make_error("failed to write value");
    };

    std::ptr::null()
}

/// Used to get value of of a Struct value fields.
///
/// NOTE: The value producing the new value must outlive the value from the field.
///
/// Safety: Values lifetimes must be valid and only support physical dtypes for now.
#[no_mangle]
pub unsafe extern "C" fn polars_value_struct_get<'a: 'b, 'b>(
    value: *mut polars_value_t<'a>,
    fieldidx: usize,
    out: *mut *mut polars_value_t<'b>,
) -> *const polars_error_t {
    let AnyValue::Struct(value_index, sarray, fields) = (*value).inner else {
        return make_error("invalid type for value");
    };

    let Some(series) = sarray.values().get(fieldidx) else {
        return make_error(format!("invalid field index {fieldidx}"));
    };

    let field = &fields[fieldidx];

    let value = match field.data_type() {
        DataType::Int64 => {
            let array = series.as_any().downcast_ref::<Int64Array>().unwrap();
            array.get(value_index).map(|val| AnyValue::Int64(val))
        }
        _ => unimplemented!("{:?}", field.data_type()),
    };

    let value = value.unwrap_or(AnyValue::Null);

    *out = Box::into_raw(Box::new(polars_value_t { inner: value }));

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
