#![allow(non_camel_case_types)]
#![allow(clippy::missing_safety_doc)]

use std::ffi::c_void;
use std::io::Write;

use polars::prelude::*;

mod expr;
mod series;
mod value;

#[no_mangle]
pub unsafe extern "C" fn polars_version(out: *mut *const u8) -> usize {
    let v = polars::VERSION;
    if !out.is_null() {
        *out = v.as_ptr();
    }
    v.len()
}

/// The callback provided for display functions, returns -1 on error.
type IOCallback =
    unsafe extern "cdecl" fn(user: *const c_void, data: *const u8, len: usize) -> isize;

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
pub fn polars_dataframe_new() -> *mut polars_dataframe_t {
    make_dataframe(DataFrame::empty())
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_new_from_series(
    series: *const *mut polars_series_t,
    nseries: usize,
    out: *mut *mut polars_dataframe_t,
) -> *const polars_error_t {
    let slice: &[*mut polars_series_t] = std::slice::from_raw_parts(series, nseries);
    let series: Vec<Series> = slice.iter().map(|s| (**s).inner.clone()).collect();
    let df = match DataFrame::new(series) {
        Ok(df) => df,
        Err(err) => return make_error(err),
    };
    *out = make_dataframe(df);
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_destroy(df: *mut polars_dataframe_t) {
    let _ = Box::from_raw(df);
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_write_parquet(
    df: *mut polars_dataframe_t,
    user: *const c_void,
    callback: IOCallback,
) -> *const polars_error_t {
    let df = &mut (*df).inner;

    let w = UserIOCallback(callback, user);
    if let Err(err) = ParquetWriter::new(w).finish(df) {
        return make_error(err);
    }

    std::ptr::null()
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

pub(crate) struct UserIOCallback(IOCallback, *const c_void);

impl std::io::Write for UserIOCallback {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = unsafe { self.0(self.1, buf.as_ptr(), buf.len()) };
        if n < 0 {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "user callback error",
            ))
        } else {
            Ok(n as usize)
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_show(
    df: *mut polars_dataframe_t,
    user: *const c_void,
    callback: IOCallback,
) {
    let df = &(*df).inner;
    let mut w = UserIOCallback(callback, user);
    write!(w, "{df}").expect("failed to show dataframe");
}

#[no_mangle]
pub unsafe extern "C" fn polars_dataframe_get(
    df: *mut polars_dataframe_t,
    name: *const u8,
    len: usize,
    out: *mut *mut polars_series_t,
) -> *const polars_error_t {
    let name = unsafe { std::slice::from_raw_parts(name, len) };
    let name = match std::str::from_utf8(name) {
        Ok(path) => path,
        Err(err) => return make_error(err),
    };

    let df = &(*df).inner;
    let mut series = match df.select_series(&[name]) {
        Ok(series) => series,
        Err(err) => return make_error(err),
    };

    let Some(series) = series.pop() else {
        return make_error(format!("dataframe has not column {name}"));
    };

    *out = series::make_series(series);

    std::ptr::null()
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
pub unsafe extern "C" fn polars_lazy_frame_sort(
    df: *mut polars_lazy_frame_t,
    exprs: *const *const polars_expr_t,
    nexprs: usize,
    descending: *const bool,
    nulls_last: bool,
    maintain_order: bool,
) {
    let exprs: Vec<Expr> = std::slice::from_raw_parts(exprs, nexprs)
        .iter()
        .map(|expr| (**expr).inner.clone())
        .collect();
    let descending = std::slice::from_raw_parts(descending, nexprs);
    let mut df = Box::from_raw(df);
    df.inner = df
        .inner
        .sort_by_exprs(&exprs, descending, nulls_last, maintain_order);
    std::mem::forget(df);
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_concat(
    lfs: *const *mut polars_lazy_frame_t,
    n: usize,
    out: *mut *mut polars_lazy_frame_t,
) -> *const polars_error_t {
    let frames: Vec<LazyFrame> = (1..n).map(|i| (**lfs.add(i)).inner.clone()).collect();

    let df = match concat(&frames, UnionArgs::default()) {
        Ok(df) => df,
        Err(err) => return make_error(err),
    };
    *out = Box::into_raw(Box::new(polars_lazy_frame_t { inner: df }));

    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_lazy_frame_select(
    df: *mut polars_lazy_frame_t,
    exprs: *const *const polars_expr_t,
    nexprs: usize,
) {
    let exprs: Vec<Expr> = std::slice::from_raw_parts(exprs, nexprs)
        .iter()
        .map(|expr| (**expr).inner.clone())
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
    let exprs: Vec<Expr> = std::slice::from_raw_parts(exprs, nexprs)
        .iter()
        .map(|expr| (**expr).inner.clone())
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
    let exprs_a: Vec<Expr> = std::slice::from_raw_parts(exprs_a, exprs_a_len)
        .iter()
        .map(|expr| (**expr).inner.clone())
        .collect();
    let exprs_b: Vec<Expr> = std::slice::from_raw_parts(exprs_b, exprs_b_len)
        .iter()
        .map(|expr| (**expr).inner.clone())
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
    n: usize,
    out: *mut *mut polars_dataframe_t,
) -> *const polars_error_t {
    let df = (*df).inner.clone();
    *out = make_dataframe(match df.fetch(n) {
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
    let exprs: Vec<Expr> = std::slice::from_raw_parts(exprs, nexprs)
        .iter()
        .map(|expr| (**expr).inner.clone())
        .collect();
    Box::into_raw(Box::new(polars_lazy_frame_t {
        inner: (*gb).inner.clone().agg(&exprs),
    }))
}
