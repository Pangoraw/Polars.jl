module Polars

include("./API.jl")

using .API

include("./expr.jl")
include("./series.jl")

function polars_error(err::Ptr{polars_error_t})
    err == C_NULL && return
    str = Ref{Ptr{UInt8}}()
    len = polars_error_message(err, str)
    message = unsafe_string(str[], len)
    polars_error_destroy(err)
    error(message)
end

mutable struct DataFrame
    ptr::Ptr{polars_dataframe_t}

    DataFrame(ptr) = finalizer(polars_dataframe_destroy, new(ptr))
end

Base.getindex(df::DataFrame, ss...) = [getindex(df, s) for s in ss] # this or select(df, ss...) ?
function Base.getindex(df::DataFrame, s::Symbol)
    s = string(s)::String
    out = Ref{Ptr{polars_series_t}}()
    series = polars_dataframe_get(df, s, length(s), out)
    Series(out[])
end

Base.unsafe_convert(::Type{Ptr{polars_dataframe_t}}, df::DataFrame) = df.ptr

mutable struct LazyFrame
    ptr::Ptr{polars_lazy_frame_t}

    LazyFrame(ptr) =
        finalizer(polars_lazy_frame_destroy, new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_frame_t}}, df::LazyFrame) = df.ptr

function lazy(df)
    out = polars_dataframe_lazy(df)
    LazyFrame(out)
end

function Base.collect(df::LazyFrame)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_lazy_frame_collect(df, out)
    polars_error(err)
    DataFrame(out[])
end

function read_parquet(path)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_dataframe_read_parquet(path, length(path), out)
    polars_error(err)
    DataFrame(out[])
end

function _show_callback(user, data, len)
    try
        n = unsafe_write(user[], data, len)
        Int(n)
    catch
        -1
    end
end

function Base.show(io::IO, df::DataFrame)
    callback = @cfunction(_show_callback, Cssize_t, (Any, Ptr{Cchar}, Cuint))
    ref = Ref(io)
    polars_dataframe_show(df.ptr, ref, callback)
end

select!(df::LazyFrame, exprs...) = select!(df, collect(exprs)::Vector)
function select!(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        polars_lazy_frame_select(df, exprs_ptrs, length(exprs_ptrs))
    end
    df
end

select(df::LazyFrame, exprs...) = select!(copy(df), exprs...)
select(df::DataFrame, exprs...) = select!(lazy(df), exprs...) |> collect

with_columns(df, exprs::Vector) = select(df, [col("*"), exprs...])
with_columns(df, exprs...) = select(df, col("*"), exprs...)

function Base.fetch(df::LazyFrame, n)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_lazy_frame_fetch(df, n, out)
    polars_error(err)
    DataFrame(out[])
end

function Base.filter!(df::LazyFrame, expr)
    polars_lazy_frame_filter(df, expr)
    df
end

Base.filter(df::LazyFrame, expr) = filter!(copy(df), expr)
Base.filter(df::DataFrame, expr) = filter!(lazy(df), expr) |> collect

function Base.copy(df::LazyFrame)
    out = polars_lazy_frame_clone(df)
    LazyFrame(out)
end

Base.join(a::DataFrame, b::DataFrame, exprs_a, exprs_b) = join(lazy(a), lazy(b), exprs_a, exprs_b) |> collect
function Base.join(a::LazyFrame, b::LazyFrame, exprs_a, exprs_b)
    exprs_a = map(ex -> ex isa String ? col(ex) : ex, exprs_a)
    exprs_a = convert(Vector{Expr}, exprs_a)
    exprs_b = map(ex -> ex isa String ? col(ex) : ex, exprs_b)
    exprs_b = convert(Vector{Expr}, exprs_b)
    @GC.preserve exprs_a exprs_b begin
        exprs_a_ptr = Ptr{polars_expr_t}[expr.ptr for expr in exprs_a]
        exprs_b_ptr = Ptr{polars_expr_t}[expr.ptr for expr in exprs_b]
        out = polars_lazy_frame_join_inner(
            a, b,
            exprs_a_ptr, length(exprs_a_ptr),
            exprs_b_ptr, length(exprs_b_ptr),
        )
    end
    LazyFrame(out)
end

mutable struct LazyGroupBy
    ptr::Ptr{polars_lazy_group_by_t}

    LazyGroupBy(ptr) =
        finalizer(polars_lazy_group_by_destroy, new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_group_by_t}}, gb::LazyGroupBy) = gb.ptr

groupby(df::LazyFrame, exprs...) = groupby(df, collect(exprs)::Vector)
function groupby(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        out = polars_lazy_frame_group_by(df, exprs_ptrs, length(exprs_ptrs))
    end
    LazyGroupBy(out)
end

agg(gb::LazyGroupBy, exprs...) = agg(gb, collect(exprs)::Vector)
function agg(gb::LazyGroupBy, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        out = polars_lazy_group_by_agg(gb, exprs_ptrs, length(exprs_ptrs))
    end
    LazyFrame(out)
end

function julia_wrapper(t)
    if t == PolarsValueTypeNull
        Missing
    elseif t ==  PolarsValueTypeBoolean
        Bool
    elseif t ==  PolarsValueTypeUInt8
        UInt8
    elseif t ==  PolarsValueTypeUInt16
        UInt16
    elseif t ==  PolarsValueTypeUInt32
        UInt32
    elseif t ==  PolarsValueTypeUInt64
        UInt64
    elseif t ==  PolarsValueTypeInt8
        Int8
    elseif t ==  PolarsValueTypeInt16
        Int16
    elseif t ==  PolarsValueTypeInt32
        Int32
    elseif t ==  PolarsValueTypeInt64
        Int64
    elseif t ==  PolarsValueTypeFloat32
        Float32
    elseif t ==  PolarsValueTypeFloat64
        Float64
    elseif t ==  PolarsValueTypeUnknown
        Nothing
    end
end

export select, with_columns, fetch,
    read_parquet, lazy, Lists, Strings,
    join, groupby, agg

end
