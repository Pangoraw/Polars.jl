module API

using CEnum

const libpolars = joinpath(@__DIR__, "../c-polars/target/debug/libpolars.so")


@cenum polars_value_type_t::UInt32 begin
    PolarsValueTypeNull = 0
    PolarsValueTypeBoolean = 1
    PolarsValueTypeUInt8 = 2
    PolarsValueTypeUInt16 = 3
    PolarsValueTypeUInt32 = 4
    PolarsValueTypeUInt64 = 5
    PolarsValueTypeInt8 = 6
    PolarsValueTypeInt16 = 7
    PolarsValueTypeInt32 = 8
    PolarsValueTypeInt64 = 9
    PolarsValueTypeFloat32 = 10
    PolarsValueTypeFloat64 = 11
    PolarsValueTypeList = 12
    PolarsValueTypeUnknown = 13
end

mutable struct polars_dataframe_t end

mutable struct polars_error_t end

mutable struct polars_expr_t end

mutable struct polars_lazy_frame_t end

mutable struct polars_lazy_group_by_t end

mutable struct polars_series_t end

mutable struct polars_value_t end

# typedef intptr_t ( * IOCallback ) ( const void * user , const uint8_t * data , uintptr_t len )
"""
The callback provided for display functions, returns -1 on error.
"""
const IOCallback = Ptr{Cvoid}

function polars_error_message(err, data)
    @ccall libpolars.polars_error_message(err::Ptr{polars_error_t}, data::Ptr{Ptr{UInt8}})::Csize_t
end

function polars_error_destroy(err)
    @ccall libpolars.polars_error_destroy(err::Ptr{polars_error_t})::Cvoid
end

function polars_dataframe_destroy(df)
    @ccall libpolars.polars_dataframe_destroy(df::Ptr{polars_dataframe_t})::Cvoid
end

function polars_dataframe_write_parquet(df, user, callback)
    @ccall libpolars.polars_dataframe_write_parquet(df::Ptr{polars_dataframe_t}, user::Ptr{Cvoid}, callback::IOCallback)::Ptr{polars_error_t}
end

function polars_dataframe_read_parquet(path, pathlen, out)
    @ccall libpolars.polars_dataframe_read_parquet(path::Ptr{UInt8}, pathlen::Csize_t, out::Ptr{Ptr{polars_dataframe_t}})::Ptr{polars_error_t}
end

function polars_dataframe_show(df, user, callback)
    @ccall libpolars.polars_dataframe_show(df::Ptr{polars_dataframe_t}, user::Ptr{Cvoid}, callback::IOCallback)::Cvoid
end

function polars_dataframe_get(df, name, len, out)
    @ccall libpolars.polars_dataframe_get(df::Ptr{polars_dataframe_t}, name::Ptr{UInt8}, len::Csize_t, out::Ptr{Ptr{polars_series_t}})::Ptr{polars_error_t}
end

function polars_dataframe_lazy(df)
    @ccall libpolars.polars_dataframe_lazy(df::Ptr{polars_dataframe_t})::Ptr{polars_lazy_frame_t}
end

function polars_lazy_frame_destroy(df)
    @ccall libpolars.polars_lazy_frame_destroy(df::Ptr{polars_lazy_frame_t})::Cvoid
end

function polars_lazy_frame_clone(df)
    @ccall libpolars.polars_lazy_frame_clone(df::Ptr{polars_lazy_frame_t})::Ptr{polars_lazy_frame_t}
end

function polars_lazy_frame_sort(df, exprs, nexprs, descending, nulls_last, maintain_order)
    @ccall libpolars.polars_lazy_frame_sort(df::Ptr{polars_lazy_frame_t}, exprs::Ptr{Ptr{polars_expr_t}}, nexprs::Csize_t, descending::Ptr{Bool}, nulls_last::Bool, maintain_order::Bool)::Cvoid
end

function polars_lazy_frame_concat(lfs, n, out)
    @ccall libpolars.polars_lazy_frame_concat(lfs::Ptr{Ptr{polars_lazy_frame_t}}, n::Csize_t, out::Ptr{Ptr{polars_lazy_frame_t}})::Ptr{polars_error_t}
end

function polars_lazy_frame_select(df, exprs, nexprs)
    @ccall libpolars.polars_lazy_frame_select(df::Ptr{polars_lazy_frame_t}, exprs::Ptr{Ptr{polars_expr_t}}, nexprs::Csize_t)::Cvoid
end

function polars_lazy_frame_filter(df, expr)
    @ccall libpolars.polars_lazy_frame_filter(df::Ptr{polars_lazy_frame_t}, expr::Ptr{polars_expr_t})::Cvoid
end

function polars_lazy_frame_collect(df, out)
    @ccall libpolars.polars_lazy_frame_collect(df::Ptr{polars_lazy_frame_t}, out::Ptr{Ptr{polars_dataframe_t}})::Ptr{polars_error_t}
end

function polars_lazy_frame_group_by(df, exprs, nexprs)
    @ccall libpolars.polars_lazy_frame_group_by(df::Ptr{polars_lazy_frame_t}, exprs::Ptr{Ptr{polars_expr_t}}, nexprs::Csize_t)::Ptr{polars_lazy_group_by_t}
end

function polars_lazy_frame_join_inner(a, b, exprs_a, exprs_a_len, exprs_b, exprs_b_len)
    @ccall libpolars.polars_lazy_frame_join_inner(a::Ptr{polars_lazy_frame_t}, b::Ptr{polars_lazy_frame_t}, exprs_a::Ptr{Ptr{polars_expr_t}}, exprs_a_len::Csize_t, exprs_b::Ptr{Ptr{polars_expr_t}}, exprs_b_len::Csize_t)::Ptr{polars_lazy_frame_t}
end

function polars_lazy_frame_fetch(df, n, out)
    @ccall libpolars.polars_lazy_frame_fetch(df::Ptr{polars_lazy_frame_t}, n::Csize_t, out::Ptr{Ptr{polars_dataframe_t}})::Ptr{polars_error_t}
end

function polars_lazy_group_by_destroy(gb)
    @ccall libpolars.polars_lazy_group_by_destroy(gb::Ptr{polars_lazy_group_by_t})::Cvoid
end

function polars_lazy_group_by_agg(gb, exprs, nexprs)
    @ccall libpolars.polars_lazy_group_by_agg(gb::Ptr{polars_lazy_group_by_t}, exprs::Ptr{Ptr{polars_expr_t}}, nexprs::Csize_t)::Ptr{polars_lazy_frame_t}
end

function polars_expr_destroy(expr)
    @ccall libpolars.polars_expr_destroy(expr::Ptr{polars_expr_t})::Cvoid
end

function polars_expr_literal_bool(value)
    @ccall libpolars.polars_expr_literal_bool(value::Bool)::Ptr{polars_expr_t}
end

function polars_expr_literal_null()
    @ccall libpolars.polars_expr_literal_null()::Ptr{polars_expr_t}
end

function polars_expr_literal_i32(value)
    @ccall libpolars.polars_expr_literal_i32(value::Int32)::Ptr{polars_expr_t}
end

function polars_expr_literal_i64(value)
    @ccall libpolars.polars_expr_literal_i64(value::Int64)::Ptr{polars_expr_t}
end

function polars_expr_literal_u32(value)
    @ccall libpolars.polars_expr_literal_u32(value::UInt32)::Ptr{polars_expr_t}
end

function polars_expr_literal_u64(value)
    @ccall libpolars.polars_expr_literal_u64(value::UInt64)::Ptr{polars_expr_t}
end

function polars_expr_literal_utf8(s, len, out)
    @ccall libpolars.polars_expr_literal_utf8(s::Ptr{UInt8}, len::Csize_t, out::Ptr{Ptr{polars_expr_t}})::Ptr{polars_error_t}
end

function polars_expr_col(name, len, out)
    @ccall libpolars.polars_expr_col(name::Ptr{UInt8}, len::Csize_t, out::Ptr{Ptr{polars_expr_t}})::Ptr{polars_error_t}
end

function polars_expr_alias(expr, name, len, out)
    @ccall libpolars.polars_expr_alias(expr::Ptr{polars_expr_t}, name::Ptr{UInt8}, len::Csize_t, out::Ptr{Ptr{polars_expr_t}})::Ptr{polars_error_t}
end

function polars_expr_keep_name(expr)
    @ccall libpolars.polars_expr_keep_name(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_sum(expr)
    @ccall libpolars.polars_expr_sum(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_product(expr)
    @ccall libpolars.polars_expr_product(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_mean(expr)
    @ccall libpolars.polars_expr_mean(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_median(expr)
    @ccall libpolars.polars_expr_median(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_min(expr)
    @ccall libpolars.polars_expr_min(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_max(expr)
    @ccall libpolars.polars_expr_max(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_arg_min(expr)
    @ccall libpolars.polars_expr_arg_min(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_arg_max(expr)
    @ccall libpolars.polars_expr_arg_max(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_nan_min(expr)
    @ccall libpolars.polars_expr_nan_min(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_nan_max(expr)
    @ccall libpolars.polars_expr_nan_max(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_floor(expr)
    @ccall libpolars.polars_expr_floor(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_ceil(expr)
    @ccall libpolars.polars_expr_ceil(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_abs(expr)
    @ccall libpolars.polars_expr_abs(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_cos(expr)
    @ccall libpolars.polars_expr_cos(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_sin(expr)
    @ccall libpolars.polars_expr_sin(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_tan(expr)
    @ccall libpolars.polars_expr_tan(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_cosh(expr)
    @ccall libpolars.polars_expr_cosh(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_sinh(expr)
    @ccall libpolars.polars_expr_sinh(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_tanh(expr)
    @ccall libpolars.polars_expr_tanh(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_n_unique(expr)
    @ccall libpolars.polars_expr_n_unique(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_unique(expr)
    @ccall libpolars.polars_expr_unique(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_count(expr)
    @ccall libpolars.polars_expr_count(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_first(expr)
    @ccall libpolars.polars_expr_first(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_last(expr)
    @ccall libpolars.polars_expr_last(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_not(expr)
    @ccall libpolars.polars_expr_not(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_is_finite(expr)
    @ccall libpolars.polars_expr_is_finite(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_is_infinite(expr)
    @ccall libpolars.polars_expr_is_infinite(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_is_nan(expr)
    @ccall libpolars.polars_expr_is_nan(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_is_null(expr)
    @ccall libpolars.polars_expr_is_null(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_is_not_null(expr)
    @ccall libpolars.polars_expr_is_not_null(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_drop_nans(expr)
    @ccall libpolars.polars_expr_drop_nans(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_drop_nulls(expr)
    @ccall libpolars.polars_expr_drop_nulls(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_implode(expr)
    @ccall libpolars.polars_expr_implode(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_flatten(expr)
    @ccall libpolars.polars_expr_flatten(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_reverse(expr)
    @ccall libpolars.polars_expr_reverse(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_eq(a, b)
    @ccall libpolars.polars_expr_eq(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_lt(a, b)
    @ccall libpolars.polars_expr_lt(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_gt(a, b)
    @ccall libpolars.polars_expr_gt(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_or(a, b)
    @ccall libpolars.polars_expr_or(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_xor(a, b)
    @ccall libpolars.polars_expr_xor(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_and(a, b)
    @ccall libpolars.polars_expr_and(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_pow(a, b)
    @ccall libpolars.polars_expr_pow(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_add(a, b)
    @ccall libpolars.polars_expr_add(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_sub(a, b)
    @ccall libpolars.polars_expr_sub(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_mul(a, b)
    @ccall libpolars.polars_expr_mul(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_div(a, b)
    @ccall libpolars.polars_expr_div(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_lengths(a)
    @ccall libpolars.polars_expr_list_lengths(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_max(a)
    @ccall libpolars.polars_expr_list_max(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_min(a)
    @ccall libpolars.polars_expr_list_min(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_arg_max(a)
    @ccall libpolars.polars_expr_list_arg_max(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_arg_min(a)
    @ccall libpolars.polars_expr_list_arg_min(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_sum(a)
    @ccall libpolars.polars_expr_list_sum(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_mean(a)
    @ccall libpolars.polars_expr_list_mean(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_reverse(a)
    @ccall libpolars.polars_expr_list_reverse(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_unique(a)
    @ccall libpolars.polars_expr_list_unique(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_unique_stable(a)
    @ccall libpolars.polars_expr_list_unique_stable(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_first(a)
    @ccall libpolars.polars_expr_list_first(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_last(a)
    @ccall libpolars.polars_expr_list_last(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_get(a, b)
    @ccall libpolars.polars_expr_list_get(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_head(a, b)
    @ccall libpolars.polars_expr_list_head(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_list_contains(a, b)
    @ccall libpolars.polars_expr_list_contains(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_to_uppercase(a)
    @ccall libpolars.polars_expr_str_to_uppercase(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_to_lowercase(a)
    @ccall libpolars.polars_expr_str_to_lowercase(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_n_chars(a)
    @ccall libpolars.polars_expr_str_n_chars(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_lengths(a)
    @ccall libpolars.polars_expr_str_lengths(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_explode(a)
    @ccall libpolars.polars_expr_str_explode(a::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_starts_with(a, b)
    @ccall libpolars.polars_expr_str_starts_with(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_ends_with(a, b)
    @ccall libpolars.polars_expr_str_ends_with(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_expr_str_contains_literal(a, b)
    @ccall libpolars.polars_expr_str_contains_literal(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
end

function polars_series_new(name, namelen, values, valueslen, out)
    @ccall libpolars.polars_series_new(name::Ptr{UInt8}, namelen::Csize_t, values::Ptr{UInt32}, valueslen::Csize_t, out::Ptr{Ptr{polars_series_t}})::Ptr{polars_error_t}
end

function polars_series_destroy(series)
    @ccall libpolars.polars_series_destroy(series::Ptr{polars_series_t})::Cvoid
end

function polars_series_type(series)
    @ccall libpolars.polars_series_type(series::Ptr{polars_series_t})::polars_value_type_t
end

function polars_series_length(series)
    @ccall libpolars.polars_series_length(series::Ptr{polars_series_t})::Csize_t
end

function polars_series_name(series, out)
    @ccall libpolars.polars_series_name(series::Ptr{polars_series_t}, out::Ptr{Ptr{UInt8}})::Csize_t
end

function polars_series_get(series, index)
    @ccall libpolars.polars_series_get(series::Ptr{polars_series_t}, index::Csize_t)::Ptr{polars_value_t}
end

function polars_series_get_bool(series, index, out)
    @ccall libpolars.polars_series_get_bool(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Bool})::Ptr{polars_error_t}
end

function polars_series_get_u8(series, index, out)
    @ccall libpolars.polars_series_get_u8(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{UInt8})::Ptr{polars_error_t}
end

function polars_series_get_u16(series, index, out)
    @ccall libpolars.polars_series_get_u16(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{UInt16})::Ptr{polars_error_t}
end

function polars_series_get_u32(series, index, out)
    @ccall libpolars.polars_series_get_u32(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{UInt32})::Ptr{polars_error_t}
end

function polars_series_get_u64(series, index, out)
    @ccall libpolars.polars_series_get_u64(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{UInt64})::Ptr{polars_error_t}
end

function polars_series_get_i8(series, index, out)
    @ccall libpolars.polars_series_get_i8(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Int8})::Ptr{polars_error_t}
end

function polars_series_get_i16(series, index, out)
    @ccall libpolars.polars_series_get_i16(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Int16})::Ptr{polars_error_t}
end

function polars_series_get_i32(series, index, out)
    @ccall libpolars.polars_series_get_i32(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Int32})::Ptr{polars_error_t}
end

function polars_series_get_i64(series, index, out)
    @ccall libpolars.polars_series_get_i64(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Int64})::Ptr{polars_error_t}
end

function polars_series_get_f32(series, index, out)
    @ccall libpolars.polars_series_get_f32(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Cfloat})::Ptr{polars_error_t}
end

function polars_series_get_f64(series, index, out)
    @ccall libpolars.polars_series_get_f64(series::Ptr{polars_series_t}, index::Csize_t, out::Ptr{Cdouble})::Ptr{polars_error_t}
end

function polars_value_type(value)
    @ccall libpolars.polars_value_type(value::Ptr{polars_value_t})::polars_value_type_t
end

function polars_value_destroy(value)
    @ccall libpolars.polars_value_destroy(value::Ptr{polars_value_t})::Cvoid
end

function polars_value_get_bool(value, out)
    @ccall libpolars.polars_value_get_bool(value::Ptr{polars_value_t}, out::Ptr{Bool})::Ptr{polars_error_t}
end

function polars_value_get_u8(value, out)
    @ccall libpolars.polars_value_get_u8(value::Ptr{polars_value_t}, out::Ptr{UInt8})::Ptr{polars_error_t}
end

function polars_value_get_u16(value, out)
    @ccall libpolars.polars_value_get_u16(value::Ptr{polars_value_t}, out::Ptr{UInt16})::Ptr{polars_error_t}
end

function polars_value_get_u32(value, out)
    @ccall libpolars.polars_value_get_u32(value::Ptr{polars_value_t}, out::Ptr{UInt32})::Ptr{polars_error_t}
end

function polars_value_get_u64(value, out)
    @ccall libpolars.polars_value_get_u64(value::Ptr{polars_value_t}, out::Ptr{UInt64})::Ptr{polars_error_t}
end

function polars_value_get_i8(value, out)
    @ccall libpolars.polars_value_get_i8(value::Ptr{polars_value_t}, out::Ptr{Int8})::Ptr{polars_error_t}
end

function polars_value_get_i16(value, out)
    @ccall libpolars.polars_value_get_i16(value::Ptr{polars_value_t}, out::Ptr{Int16})::Ptr{polars_error_t}
end

function polars_value_get_i32(value, out)
    @ccall libpolars.polars_value_get_i32(value::Ptr{polars_value_t}, out::Ptr{Int32})::Ptr{polars_error_t}
end

function polars_value_get_i64(value, out)
    @ccall libpolars.polars_value_get_i64(value::Ptr{polars_value_t}, out::Ptr{Int64})::Ptr{polars_error_t}
end

function polars_value_get_f32(value, out)
    @ccall libpolars.polars_value_get_f32(value::Ptr{polars_value_t}, out::Ptr{Cfloat})::Ptr{polars_error_t}
end

function polars_value_get_f64(value, out)
    @ccall libpolars.polars_value_get_f64(value::Ptr{polars_value_t}, out::Ptr{Cdouble})::Ptr{polars_error_t}
end

"""
    polars_value_list_get(value, out)

Returns the value as a Series when the dtype of the value is a list.
"""
function polars_value_list_get(value, out)
    @ccall libpolars.polars_value_list_get(value::Ptr{polars_value_t}, out::Ptr{Ptr{polars_series_t}})::Ptr{polars_error_t}
end

"""
    polars_value_list_type(value)

Returns the element type of the provided value which must be a list. The value type is PolarsValueTypeUnknown if the value is not a list so makes sure it is one otherwise, you cannot differentiate between list<unkown> and unkown.
"""
function polars_value_list_type(value)
    @ccall libpolars.polars_value_list_type(value::Ptr{polars_value_t})::polars_value_type_t
end

# exports
const PREFIXES = ["polars_", "Polars"]
for name in names(@__MODULE__; all=true), prefix in PREFIXES
    if startswith(string(name), prefix)
        @eval export $name
    end
end

end # module
