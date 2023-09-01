use polars::{
    lazy::dsl::{string::StringNameSpace, ListNameSpace},
    prelude::*,
};

use crate::*;

#[no_mangle]
pub unsafe extern "C" fn polars_expr_destroy(expr: *const polars_expr_t) {
    assert!(!expr.is_null());
    let _ = Box::from_raw(expr.cast_mut());
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_bool(value: bool) -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::Boolean(value)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_null() -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::Null),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_i32(value: i32) -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::Int32(value)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_i64(value: i64) -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::Int64(value)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_u32(value: u32) -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::UInt32(value)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_u64(value: u64) -> *const polars_expr_t {
    Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::UInt64(value)),
    }))
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_literal_utf8(
    s: *const u8,
    len: usize,
    out: *mut *const polars_expr_t,
) -> *const polars_error_t {
    let value = match std::str::from_utf8(std::slice::from_raw_parts(s, len)) {
        Ok(value) => value,
        Err(err) => return make_error(err),
    };
    *out = Box::into_raw(Box::new(polars_expr_t {
        inner: Expr::Literal(LiteralValue::Utf8(value.to_owned())),
    }));
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_col(
    name: *const u8,
    len: usize,
    out: *mut *const polars_expr_t,
) -> *const polars_error_t {
    let name = match std::str::from_utf8(std::slice::from_raw_parts(name, len)) {
        Ok(value) => value,
        Err(err) => return make_error(err),
    };
    let expr = col(name);
    *out = Box::into_raw(Box::new(polars_expr_t { inner: expr }));
    std::ptr::null()
}

#[no_mangle]
pub unsafe extern "C" fn polars_expr_alias(
    expr: *const polars_expr_t,
    name: *const u8,
    len: usize,
    out: *mut *const polars_expr_t,
) -> *const polars_error_t {
    let name = match std::str::from_utf8(std::slice::from_raw_parts(name, len)) {
        Ok(value) => value,
        Err(err) => return make_error(err),
    };
    let aliased = (*expr).inner.clone().alias(name);
    *out = Box::into_raw(Box::new(polars_expr_t { inner: aliased }));
    std::ptr::null()
}

macro_rules! gen_impl_expr {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(expr: *const polars_expr_t) -> *const polars_expr_t {
            let expr = &(*expr).inner;
            let out_expr = $t(expr.clone());
            Box::into_raw(Box::new(polars_expr_t { inner: out_expr }))
        }
    };
}

gen_impl_expr!(polars_expr_keep_name, Expr::keep_name);

gen_impl_expr!(polars_expr_sum, Expr::sum);
gen_impl_expr!(polars_expr_product, Expr::product);
gen_impl_expr!(polars_expr_mean, Expr::mean);
gen_impl_expr!(polars_expr_median, Expr::median);
gen_impl_expr!(polars_expr_min, Expr::min);
gen_impl_expr!(polars_expr_max, Expr::max);
gen_impl_expr!(polars_expr_arg_min, Expr::arg_min);
gen_impl_expr!(polars_expr_arg_max, Expr::arg_max);
gen_impl_expr!(polars_expr_nan_min, Expr::nan_min);
gen_impl_expr!(polars_expr_nan_max, Expr::nan_max);

gen_impl_expr!(polars_expr_floor, Expr::floor);
gen_impl_expr!(polars_expr_ceil, Expr::ceil);
gen_impl_expr!(polars_expr_abs, Expr::abs);
gen_impl_expr!(polars_expr_cos, Expr::cos);
gen_impl_expr!(polars_expr_sin, Expr::sin);
gen_impl_expr!(polars_expr_tan, Expr::tan);
gen_impl_expr!(polars_expr_cosh, Expr::cosh);
gen_impl_expr!(polars_expr_sinh, Expr::sinh);
gen_impl_expr!(polars_expr_tanh, Expr::tanh);

gen_impl_expr!(polars_expr_n_unique, Expr::n_unique);
gen_impl_expr!(polars_expr_unique, Expr::unique);
gen_impl_expr!(polars_expr_count, Expr::count);
gen_impl_expr!(polars_expr_first, Expr::first);
gen_impl_expr!(polars_expr_last, Expr::last);

gen_impl_expr!(polars_expr_not, Expr::not);
gen_impl_expr!(polars_expr_is_finite, Expr::is_finite);
gen_impl_expr!(polars_expr_is_infinite, Expr::is_infinite);
gen_impl_expr!(polars_expr_is_nan, Expr::is_nan);
gen_impl_expr!(polars_expr_is_null, Expr::is_null);
gen_impl_expr!(polars_expr_is_not_null, Expr::is_not_null);
gen_impl_expr!(polars_expr_drop_nans, Expr::drop_nans);
gen_impl_expr!(polars_expr_drop_nulls, Expr::drop_nulls);

gen_impl_expr!(polars_expr_implode, Expr::implode);
gen_impl_expr!(polars_expr_flatten, Expr::flatten);
gen_impl_expr!(polars_expr_reverse, Expr::reverse);

macro_rules! gen_impl_expr_binary {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(
            a: *const polars_expr_t,
            b: *const polars_expr_t,
        ) -> *const polars_expr_t {
            let a = &(*a).inner;
            let b = &(*b).inner;
            let out_expr = $t(a.clone(), b.clone());
            Box::into_raw(Box::new(polars_expr_t { inner: out_expr }))
        }
    };
}

gen_impl_expr_binary!(polars_expr_eq, Expr::eq);
gen_impl_expr_binary!(polars_expr_lt, Expr::lt);
gen_impl_expr_binary!(polars_expr_gt, Expr::gt);
gen_impl_expr_binary!(polars_expr_or, Expr::or);
gen_impl_expr_binary!(polars_expr_xor, Expr::xor);
gen_impl_expr_binary!(polars_expr_and, Expr::and);

gen_impl_expr_binary!(polars_expr_pow, Expr::pow);
gen_impl_expr_binary!(polars_expr_add, core::ops::Add::add);
gen_impl_expr_binary!(polars_expr_sub, core::ops::Sub::sub);
gen_impl_expr_binary!(polars_expr_mul, core::ops::Mul::mul);
gen_impl_expr_binary!(polars_expr_div, core::ops::Div::div);

macro_rules! gen_impl_expr_list {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(a: *const polars_expr_t) -> *const polars_expr_t {
            let expr = $t((*a).inner.clone().list());
            Box::into_raw(Box::new(polars_expr_t { inner: expr }))
        }
    };
}

gen_impl_expr_list!(polars_expr_list_lengths, ListNameSpace::lengths);
gen_impl_expr_list!(polars_expr_list_max, ListNameSpace::max);
gen_impl_expr_list!(polars_expr_list_min, ListNameSpace::min);
gen_impl_expr_list!(polars_expr_list_arg_max, ListNameSpace::arg_max);
gen_impl_expr_list!(polars_expr_list_arg_min, ListNameSpace::arg_min);
gen_impl_expr_list!(polars_expr_list_sum, ListNameSpace::sum);
gen_impl_expr_list!(polars_expr_list_mean, ListNameSpace::mean);
gen_impl_expr_list!(polars_expr_list_reverse, ListNameSpace::reverse);
gen_impl_expr_list!(polars_expr_list_unique, ListNameSpace::unique);
gen_impl_expr_list!(polars_expr_list_unique_stable, ListNameSpace::unique_stable);
gen_impl_expr_list!(polars_expr_list_first, ListNameSpace::first);
gen_impl_expr_list!(polars_expr_list_last, ListNameSpace::last);

macro_rules! gen_impl_expr_binary_list {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(
            a: *const polars_expr_t,
            b: *const polars_expr_t,
        ) -> *const polars_expr_t {
            let expr = $t((*a).inner.clone().list(), ((*b).inner.clone()));
            Box::into_raw(Box::new(polars_expr_t { inner: expr }))
        }
    };
}

gen_impl_expr_binary_list!(polars_expr_list_get, ListNameSpace::get);
gen_impl_expr_binary_list!(polars_expr_list_head, ListNameSpace::head);
gen_impl_expr_binary_list!(polars_expr_list_contains, ListNameSpace::contains);

macro_rules! gen_impl_expr_str {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(a: *const polars_expr_t) -> *const polars_expr_t {
            let expr = $t((*a).inner.clone().str());
            Box::into_raw(Box::new(polars_expr_t { inner: expr }))
        }
    };
}

gen_impl_expr_str!(polars_expr_str_to_uppercase, StringNameSpace::to_uppercase);
gen_impl_expr_str!(polars_expr_str_to_lowercase, StringNameSpace::to_lowercase);
#[cfg(feature = "nightly")]
gen_impl_expr_str!(polars_expr_str_to_titlecase, StringNameSpace::to_titlecase);
gen_impl_expr_str!(polars_expr_str_n_chars, StringNameSpace::n_chars);
gen_impl_expr_str!(polars_expr_str_lengths, StringNameSpace::lengths);
gen_impl_expr_str!(polars_expr_str_explode, StringNameSpace::explode);

macro_rules! gen_impl_expr_binary_str {
    ($n: ident, $t: expr) => {
        #[no_mangle]
        pub unsafe extern "C" fn $n(
            a: *const polars_expr_t,
            b: *const polars_expr_t,
        ) -> *const polars_expr_t {
            let expr = $t((*a).inner.clone().str(), ((*b).inner.clone()));
            Box::into_raw(Box::new(polars_expr_t { inner: expr }))
        }
    };
}

gen_impl_expr_binary_str!(polars_expr_str_starts_with, StringNameSpace::starts_with);
gen_impl_expr_binary_str!(polars_expr_str_ends_with, StringNameSpace::ends_with);
gen_impl_expr_binary_str!(
    polars_expr_str_contains_literal,
    StringNameSpace::contains_literal
);
