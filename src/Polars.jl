module Polars

import PrettyTables, Tables

const MaybeMissing{T} = Union{T,Union{T,Missing}}
const PhysicalDType = Union{Bool,Int8,Int16,Int32,Int64,UInt8,
                            UInt16,UInt32,UInt64,Float32,Float64}

nomissing(::Type{MaybeMissing{T}}) where {T} = T
nomissing(::Type{T}) where {T} = T

"Internal function to write back to an IO from rustland"
function _write_callback(user, data, len)
    try
        n = unsafe_write(user isa IO ? user : user[], data, len)
        Int(n)
    catch
        -1
    end
end


include("./API.jl")

using .API

include("./arrow.jl")
include("./expr.jl")
include("./series.jl")
include("./value.jl")

"""
    version()::VersionNumber

Returns the rust Polars version with which the C-API was built.
"""
function version()
    out = Ref{Ptr{UInt8}}()
    len = polars_version(out)
    ver = unsafe_string(out[], len)
    VersionNumber(ver)
end

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

    DataFrame(ptr::Ptr{polars_dataframe_t}) =
        finalizer(polars_dataframe_destroy, new(ptr))
end

"""
    DataFrame(table)

A wrapper around an immutable polars dataframe object.
"""
function DataFrame(table)
    array, schema = Polars.arrowtable(table, "polars.dataframe")
    try
        df = API.polars_dataframe_new_from_carrow(schema, array)
        DataFrame(df)
    finally
        release_schema!(schema)
    end
end

function Base.size(df::DataFrame)
    rows, cols = Ref{Csize_t}(), Ref{Csize_t}()
    API.polars_dataframe_size(df, rows, cols)
    (Int(rows[]), Int(cols[]))
end

Base.getindex(df::DataFrame, ss...) = [getindex(df, s) for s in ss] # this or select(df, ss...) ?
Base.getindex(df::DataFrame, idx::Int) = Tables.getcolumn(df, idx)
Base.getindex(df::DataFrame, s::String) = getindex(df, Symbol(s))
function Base.getindex(df::DataFrame, s::Symbol)
    s = string(s)::String
    out = Ref{Ptr{polars_series_t}}()
    err = polars_dataframe_get(df, s, length(s), out)
    polars_error(err)
    Series(out[])
end

Base.unsafe_convert(::Type{Ptr{polars_dataframe_t}}, df::DataFrame) = df.ptr

mutable struct LazyFrame
    ptr::Ptr{polars_lazy_frame_t}

    LazyFrame(ptr) =
        finalizer(polars_lazy_frame_destroy, new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_frame_t}}, df::LazyFrame) = df.ptr

"""
    lazy(df::DataFrame)::LazyFrame

Returns a lazy frame over the provided dataframe.

See also [`collect`](@ref).
"""
function lazy(df)
    out = polars_dataframe_lazy(df)
    LazyFrame(out)
end

"""
    collect(lf::LazyFrame)::DataFrame

Materializes the lazy frame as a DataFrame.
"""
function Base.collect(df::LazyFrame)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_lazy_frame_collect(df, out)
    polars_error(err)
    DataFrame(out[])
end

"""
    read_parquet(path::String)::DataFrame

Reads a dataframe stored in a parquet file.
"""
function read_parquet(path)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_dataframe_read_parquet(path, length(path), out)
    polars_error(err)
    DataFrame(out[])
end

"""
    write_parquet(io::IO, df::DataFrame)
    write_parquet(path::String, df::DataFrame)

Writes a dataframe to a parquet file provided as an `IO`.
"""
function write_parquet(io::IO, df::DataFrame)
    callback = @cfunction(_write_callback, Cssize_t, (Any, Ptr{Cchar}, Cuint))
    ref = Ref(io)
    err = polars_dataframe_write_parquet(df, ref, callback)
    polars_error(err)
    nothing
end
write_parquet(p::String, df::DataFrame) = open(io -> write_parquet(io, df), p, "w")

Base.summary(df::DataFrame) = join(size(df), '×') * " DataFrame"

function Base.show(io::IO, df::DataFrame)
    # Copied from the nice PrettyTables setup in DataFrames.jl
    # https://github.com/JuliaData/DataFrames.jl/blob/e341cc7873a08977cc8e4d56f28303883582c920/src/abstractdataframe/show.jl#L253-L279
    # Still needs some tuning/options
    PrettyTables.pretty_table(io, df;
        title=Base.summary(df),
        hlines=[:header],
        compact_printing=true,
        crop=:both,
        maximum_columns_width=32,
        vlines=Int[],
        # show_row_number=true,
        header_alignment=:l,
        row_number_alignment=:r,
    )
end

_select!(df::LazyFrame, exprs...) = _select!(df, collect(exprs)::Vector)
function _select!(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        polars_lazy_frame_select(df, exprs_ptrs, length(exprs_ptrs))
    end
    df
end

"""
    select(lf::LazyFrame, exprs...)::LazyFrame
    select(df::DataFrame, exprs...)::DataFrame

Select a fixed set of expressions from the provided frames.
"""
select(df::LazyFrame, exprs...) = _select!(clone(df), exprs...)
select(df::DataFrame, exprs...) = _select!(lazy(df), exprs...) |> collect

"""
    with_columns(lf::LazyFrame, exprs...)::LazyFrame
    with_columns(df::DataFrame, exprs...)::DataFrame

Select a fixed set of expressions from the provided frames and
also returns the existing columns.

```julia-repl
julia> df = DataFrame((; x=[1,2,3]))
3×1 DataFrame
 x      
 Int64? 
────────
      1
      2
      3

julia> with_columns(df, col("x") * 2 |> alias("2x"))
3×2 DataFrame
 x       2x     
 Int64?  Int64? 
────────────────
      1       2
      2       4
      3       6
```
"""
with_columns(df::LazyFrame, exprs...) = _with_columns!(clone(df), collect(exprs)::Vector)
with_columns(df::DataFrame, exprs...) = _with_columns!(lazy(df), collect(exprs)::Vector) |> collect

function _with_columns!(df::LazyFrame, exprs::Vector)
    exprs = map(ex -> ex isa String ? col(ex) : ex, exprs)
    exprs = convert(Vector{Expr}, exprs)
    @GC.preserve exprs begin
        exprs_ptrs = Ptr{polars_expr_t}[expr.ptr for expr in exprs]
        polars_lazy_frame_with_columns(df, exprs_ptrs, length(exprs_ptrs))
    end
    df
end

"""
    fetch(lf::LazyFrame, n)::DataFrame

Fetches the `n` first samples from the provided lazy frame and
collect them in a `DataFrame`.
"""
function Base.fetch(df::LazyFrame, n)
    out = Ref{Ptr{polars_dataframe_t}}()
    err = polars_lazy_frame_fetch(df, n, out)
    polars_error(err)
    DataFrame(out[])
end

function _filter!(df::LazyFrame, expr)
    polars_lazy_frame_filter(df, expr)
    df
end

"""
    filter(lf::LazyFrame, expr)
    filter(df::DataFrame, expr)

Filters the rows of the provided frames based on the provided expression.
"""
Base.filter(df::LazyFrame, expr) = _filter!(clone(df), expr)
Base.filter(df::DataFrame, expr) = _filter!(lazy(df), expr) |> collect

function clone(df::LazyFrame)
    out = polars_lazy_frame_clone(df)
    LazyFrame(out)
end

innerjoin(a, b, expr) = innerjoin(a, b, expr, expr)
innerjoin(a::DataFrame, b::DataFrame, exprs_a, exprs_b) = innerjoin(lazy(a), lazy(b), exprs_a, exprs_b) |> collect
innerjoin(a::LazyFrame, b::LazyFrame, expr_a, expr_b) = innerjoin(a, b, [expr_a], [expr_b])
function innerjoin(a::LazyFrame, b::LazyFrame, exprs_a::Vector, exprs_b::Vector)
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

"""
    LazyGroupBy()

A groupby over a [`LazyFrame`] whose values can be aggregated using the
[`agg`](@ref) function.
"""
mutable struct LazyGroupBy
    ptr::Ptr{polars_lazy_group_by_t}

    LazyGroupBy(ptr) =
        finalizer(polars_lazy_group_by_destroy, new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_lazy_group_by_t}}, gb::LazyGroupBy) = gb.ptr

"""
    groupby(df::LazyFrame, exprs...)

Returns a lazy groupby object over the provided [`LazyFrame`](@ref).
The values for the groupby can be aggregated using the [`agg`](@ref) function.
"""
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

"""
    agg(gb, exprs...)::LazyFrame

Aggregates the value over the groupby object and return a resulting [`LazyFrame`](@ref).
"""
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

export Series, DataFrame,
       select, with_columns, fetch,
       read_parquet, write_parquet,
       lazy, innerjoin, groupby, agg

## Tables.jl interface

import Tables: schema

function schema(df::DataFrame)
    schema = API.polars_dataframe_schema(df)
    load_dataframe_schema(schema)
end

Tables.istable(::DataFrame) = true

Tables.columnaccess(::DataFrame) = true
Tables.rowaccess(::DataFrame) = true # enables Pluto.jl viewer

Tables.columns(df::DataFrame) = df

Tables.columnnames(df::DataFrame) = schema(df).names
Tables.getcolumn(df::DataFrame, col::Symbol) = getindex(df, col)
Tables.getcolumn(df::DataFrame, idx::Int) = Tables.getcolumn(df, Tables.columnnames(df)[idx])

end # module Polars
