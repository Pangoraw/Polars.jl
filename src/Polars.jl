module Polars

# TODO: generate libpolars_jll
const libpolars = "c-polars/target/debug/libpolars.so"

struct polars_error_t end
struct polars_series_t end
struct polars_dataframe_t end
struct polars_lazy_frame_t end
struct polars_lazy_group_by_t end
struct polars_expr_t end

include("./expr.jl")
include("./series.jl")

function polars_error(err::Ptr{polars_error_t})
    err == C_NULL && return
    str = Ref{Ptr{Cchar}}()
    len = @ccall libpolars.polars_error_message(err::Ptr{polars_error_t}, str::Ptr{Ptr{Cchar}})::Cuint
    message = unsafe_string(str[], len)
    @ccall libpolars.polars_error_destroy(err::Ptr{polars_error_t})::Cvoid
    error(message)
end

mutable struct DataFrame
    ptr::Ptr{polars_dataframe_t}

    DataFrame(ptr) =
        finalizer(new(ptr)) do df
            @ccall libpolars.polars_dataframe_destroy(df::Ptr{polars_dataframe_t})::Cvoid
        end
end

Base.unsafe_convert(::Type{Ptr{polars_dataframe_t}}, df::DataFrame) = df.ptr

mutable struct LazyFrame
    ptr::Ptr{polars_lazy_frame_t}

    LazyFrame(ptr) =
        finalizer(new(ptr)) do df
            @ccall libpolars.polars_lazy_frame_destroy(df::Ptr{polars_lazy_frame_t})::Cvoid
        end
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_frame_t}}, df::LazyFrame) = df.ptr

function lazy(df)
    out = @ccall libpolars.polars_dataframe_lazy(df::Ptr{polars_dataframe_t})::Ptr{polars_lazy_frame_t}
    LazyFrame(out)
end

function Base.collect(df::LazyFrame)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = @ccall libpolars.polars_lazy_frame_collect(df::Ptr{polars_lazy_frame_t}, out::Ptr{Ptr{polars_dataframe_t}})::Ptr{polars_error_t}
    polars_error(err)
    DataFrame(out[])
end

function read_parquet(path)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = @ccall libpolars.polars_dataframe_read_parquet(
        path::Ptr{Cchar}, length(path)::Cuint,
        out::Ptr{Ptr{polars_dataframe_t}},
    )::Ptr{polars_error_t}
    polars_error(err)
    DataFrame(out[])
end

function _show_callback(user, data, len)
    s = unsafe_string(data, len)
    write(user[], s)
    nothing
end

function Base.show(io::IO, df::DataFrame)
    callback = @cfunction(_show_callback, Cvoid, (Any, Ptr{Cchar}, Cuint))
    ref = Ref(io)
    @ccall libpolars.polars_dataframe_show(
        df.ptr::Ptr{polars_dataframe_t}, ref::Any,
        callback::Ptr{Cvoid},
    )::Cvoid
end

select!(df::LazyFrame, exprs...) = select!(df, collect(exprs)::Vector)
function select!(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        @ccall libpolars.polars_lazy_frame_select(
            df::Ptr{polars_lazy_frame_t},
            exprs_ptrs::Ptr{Ptr{polars_expr_t}},
            length(exprs_ptrs)::Cint,
        )::Cvoid
    end
    df
end

select(df::LazyFrame, exprs...) = select!(copy(df), exprs...)
select(df::DataFrame, exprs...) = select!(lazy(df), exprs...) |> collect

with_columns(df, exprs::Vector) = select(df, [col("*"), exprs...])
with_columns(df, exprs...) = select(df, col("*"), exprs...)

function Base.fetch(df::LazyFrame, n)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = @ccall libpolars.polars_lazy_frame_fetch(
        df::Ptr{polars_lazy_frame_t},
        n::UInt32,
        out::Ptr{Ptr{polars_dataframe_t}},
    )::Ptr{polars_error_t}
    polars_error(err)
    DataFrame(out[])
end

function Base.filter!(df::LazyFrame, expr)
    @ccall libpolars.polars_lazy_frame_filter(
        df::Ptr{polars_lazy_frame_t},
        expr::Ptr{polars_expr_t},
    )::Cvoid
    df
end

Base.filter(df::LazyFrame, expr) = filter!(copy(df), expr)
Base.filter(df::DataFrame, expr) = filter!(lazy(df), expr) |> collect

function Base.copy(df::LazyFrame)
    out = @ccall libpolars.polars_lazy_frame_clone(
        df::Ptr{polars_lazy_frame_t}
    )::Ptr{polars_lazy_frame_t}
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
        out = @ccall libpolars.polars_lazy_frame_join_inner(
            a::Ptr{polars_lazy_frame_t}, b::Ptr{polars_lazy_frame_t},
            exprs_a_ptr::Ptr{Ptr{polars_expr_t}}, length(exprs_a_ptr)::Csize_t,
            exprs_b_ptr::Ptr{Ptr{polars_expr_t}}, length(exprs_b_ptr)::Csize_t,
        )::Ptr{polars_lazy_frame_t}
    end
    LazyFrame(out)
end

mutable struct LazyGroupBy
    ptr::Ptr{polars_lazy_group_by_t}

    LazyGroupBy(ptr) =
        finalizer(new(ptr)) do gb
            @ccall libpolars.polars_lazy_group_by_destroy(gb::Ptr{polars_lazy_frame_t})::Cvoid
        end
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_group_by_t}}, gb::LazyGroupBy) = gb.ptr

groupby(df::LazyFrame, exprs...) = groupby(df, collect(exprs)::Vector)
function groupby(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        out = @ccall libpolars.polars_lazy_frame_group_by(
            df::Ptr{polars_lazy_frame_t},
            exprs_ptrs::Ptr{Ptr{polars_expr_t}},
            length(exprs_ptrs)::Cint,
        )::Ptr{polars_lazy_group_by_t}
    end
    LazyGroupBy(out)
end

agg(gb::LazyGroupBy, exprs...) = agg(gb, collect(exprs)::Vector)
function agg(gb::LazyGroupBy, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        out = @ccall libpolars.polars_lazy_group_by_agg(
            gb::Ptr{polars_lazy_group_by_t},
            exprs_ptrs::Ptr{Ptr{polars_expr_t}},
            length(exprs_ptrs)::Cint,
        )::Ptr{polars_lazy_frame_t}
    end
    LazyFrame(out)
end

export select, with_columns, fetch,
    read_parquet, lazy, Lists, Strings,
    join, groupby, agg

end
