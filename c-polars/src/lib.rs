#![allow(non_camel_case_types)]

use std::ffi::c_void;

use polars::prelude::*;

mod expr;

type IOCallback = unsafe extern "cdecl" fn(user: *const c_void, data: *const u8, len: usize);

pub struct polars_error_t {
    msg: String,
}

fn make_error<E: ToString>(err: E) -> *const polars_error_t {
    Box::into_raw(Box::new(polars_error_t {
        msg: err.to_string(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_error_message(
    err: *const polars_error_t,
    data: *mut *const u8,
) -> usize {
    assert!(!err.is_null());
    assert!(!data.is_null());
    *data = (*err).msg.as_ptr();
    return (*err).msg.len();
}

#[no_mangle]
pub unsafe extern "C" fn polars_error_destroy(err: *const polars_error_t) {
    assert!(!err.is_null());
    let _ = Box::from_raw(err.cast_mut());
}

// TODO: investigate what the lifetime implies.
pub struct polars_value_t<'a> {
    inner: AnyValue<'a>,
}

#[no_mangle]
pub unsafe extern "C" fn polars_value_destroy(value: *mut polars_value_t) {
    assert!(!value.is_null());
    let _ = Box::from_raw(value);
}

#[no_mangle]
pub unsafe extern "C" fn polars_value_get_u32(
    value: *mut polars_value_t,
    out: *mut u32,
) -> *const polars_error_t {
    match (*value).inner {
        AnyValue::UInt32(value) => *out = value,
        _ => {
            return make_error("value is not of type u32");
        }
    }
    std::ptr::null()
}

pub struct polars_dataframe_t {
    inner: DataFrame,
}

pub struct polars_lazy_frame_t {
    inner: LazyFrame,
}

pub struct polars_lazy_group_by_t {
    inner: LazyGroupBy,
}

pub struct polars_series_t {
    inner: Series,
}

pub struct polars_expr_t {
    inner: Expr,
}

fn make_dataframe(df: DataFrame) -> *mut polars_dataframe_t {
    Box::into_raw(Box::new(polars_dataframe_t { inner: df }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_new(
    name: *const u8,
    namelen: usize,
    values: *const u32,
    valueslen: usize,
    out: *mut *mut polars_series_t,
) -> *const polars_error_t {
    let name = match std::str::from_utf8(std::slice::from_raw_parts(name, namelen)) {
        Ok(name) => name,
        Err(err) => {
            return make_error(err);
        }
    };
    let series = Series::new(name, std::slice::from_raw_parts(values, valueslen));
    *out = Box::into_raw(Box::new(polars_series_t { inner: series }));
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_destroy(series: *mut polars_series_t) {
    assert!(!series.is_null());
    let _ = Box::from_raw(series);
}

#[no_mangle]
pub unsafe extern "C" fn polars_series_length(series: *mut polars_series_t) -> usize {
    assert!(!series.is_null());
    (*series).inner.len()
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

#[no_mangle]
pub unsafe extern "C" fn polars_series_get_u32(
    series: *mut polars_series_t,
    index: usize,
    out: *mut u32,
) -> *const polars_error_t {
    assert!(!series.is_null());
    match (*series).inner.get(index) {
        Ok(AnyValue::UInt32(value)) => {
            *out = value;
            std::ptr::null()
        }
        Ok(_) => make_error("series type is invalid"),
        Err(err) => make_error(err),
    }
}

#[no_mangle]
pub fn polars_dataframe_new() -> *mut polars_dataframe_t {
    make_dataframe(DataFrame::empty())
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_destroy(df: *mut polars_dataframe_t) {
    let _ = Box::from_raw(df);
}

#[no_mangle]
pub extern "C" fn polars_dataframe_read_parquet(
    path: *const u8,
    pathlen: usize,
    out: *mut *mut polars_dataframe_t,
) -> *const polars_error_t {
    let path = unsafe { std::slice::from_raw_parts(path, pathlen) };
    let path = match std::str::from_utf8(path) {
        Ok(path) => path,
        Err(err) => return make_error(err),
    };

    let file = match std::fs::OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(err) => return make_error(err),
    };

    match ParquetReader::new(file).finish() {
        Ok(df) => unsafe {
            *out = make_dataframe(df);
        },
        Err(err) => return make_error(err),
    }

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_show(
    df: *mut polars_dataframe_t,
    user: *const c_void,
    callback: IOCallback,
) {
    // TODO: can we have streaming here?
    let df = &(*df).inner;
    let s = format!("{}", df);
    callback(user, s.as_ptr(), s.len());
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_lazy(
    df: *mut polars_dataframe_t,
) -> *mut polars_lazy_frame_t {
    let df = &(*df).inner;
    Box::into_raw(Box::new(polars_lazy_frame_t {
        inner: df.clone().lazy(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_destroy(df: *mut polars_lazy_frame_t) {
    assert!(!df.is_null());
    let _ = Box::from_raw(df);
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_clone(
    df: *mut polars_lazy_frame_t,
) -> *mut polars_lazy_frame_t {
    assert!(!df.is_null());
    Box::into_raw(Box::new(polars_lazy_frame_t {
        inner: (*df).inner.clone(),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_select(
    df: *mut polars_lazy_frame_t,
    exprs: *const *const polars_expr_t,
    nexprs: usize,
) {
    let exprs: Vec<Expr> = (0..nexprs as usize)
        .map(|i| {
            let expr = &(**exprs.add(i));
            expr.inner.clone()
        })
        .collect();
    let mut df = Box::from_raw(df);
    df.inner = df.inner.select(&exprs);
    std::mem::forget(df);
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_filter(
    df: *mut polars_lazy_frame_t,
    expr: *const polars_expr_t,
) {
    assert!(!df.is_null());
    assert!(!expr.is_null());
    let mut df = Box::from_raw(df);
    df.inner = df.inner.filter((*expr).inner.clone()); // NOTE: we clone the expr here, can we assume
                                                       // that the function takes ownership of it?
    std::mem::forget(df);
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_collect(
    df: *mut polars_lazy_frame_t,
    out: *mut *mut polars_dataframe_t,
) -> *const polars_error_t {
    let df = (*df).inner.clone();
    *out = make_dataframe(match df.collect() {
        Ok(value) => value,
        Err(err) => return make_error(err),
    });
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_group_by(
    df: *mut polars_lazy_frame_t,
    exprs: *const *const polars_expr_t,
    nexprs: usize,
) -> *mut polars_lazy_group_by_t {
    let exprs: Vec<Expr> = (0..nexprs as usize)
        .map(|i| {
            let expr = &(**exprs.add(i));
            expr.inner.clone()
        })
        .collect();
    let gb = (*df).inner.clone().groupby(&exprs);
    Box::into_raw(Box::new(polars_lazy_group_by_t { inner: gb }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_join_inner(
    a: *mut polars_lazy_frame_t,
    b: *mut polars_lazy_frame_t,
    exprs_a: *const *const polars_expr_t,
    exprs_a_len: usize,
    exprs_b: *const *const polars_expr_t,
    exprs_b_len: usize,
) -> *mut polars_lazy_frame_t {
    let exprs_a: Vec<Expr> = (0..exprs_a_len as usize)
        .map(|i| {
            let expr = &(**exprs_a.add(i));
            expr.inner.clone()
        })
        .collect();
    let exprs_b: Vec<Expr> = (0..exprs_b_len as usize)
        .map(|i| {
            let expr = &(**exprs_b.add(i));
            expr.inner.clone()
        })
        .collect();
    let df = LazyFrame::join(
        (*a).inner.clone(),
        (*b).inner.clone(),
        exprs_a,
        exprs_b,
        JoinArgs::new(JoinType::Inner),
    );
    Box::into_raw(Box::new(polars_lazy_frame_t { inner: df }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_fetch(
    df: *mut polars_lazy_frame_t,
    n: u32,
    out: *mut *mut polars_dataframe_t,
) -> *const polars_error_t {
    let df = (*df).inner.clone();
    *out = make_dataframe(match df.fetch(n as usize) {
        Ok(value) => value,
        Err(err) => return make_error(err),
    });
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_group_by_destroy(gb: *const polars_lazy_group_by_t) {
    assert!(!gb.is_null());
    let _ = Box::from_raw(gb.cast_mut());
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_group_by_agg(
    gb: *mut polars_lazy_group_by_t,
    exprs: *const *const polars_expr_t,
    nexprs: usize,
) -> *mut polars_lazy_frame_t {
    let exprs: Vec<Expr> = (0..nexprs as usize)
        .map(|i| {
            let expr = &(**exprs.add(i));
            expr.inner.clone()
        })
        .collect();
    Box::into_raw(Box::new(polars_lazy_frame_t {
        inner: (*gb).inner.clone().agg(&exprs),
    }))
}
