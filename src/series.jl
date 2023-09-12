"""
    Series(name::String, values::Vector{T})::Series{T}

A series is a collection of values used as columns inside a [`DataFrame`](@ref).
"""
mutable struct Series{T} <: AbstractVector{T}
    ptr::Ptr{polars_series_t}
    null_count::Int
    length::Int

    function Series(ptr)
        @assert ptr != C_NULL

        schema = polars_series_schema(ptr)
        _, T = load_series_schema(schema)

        len = polars_series_length(ptr)
        null_count = polars_series_null_count(ptr)

        T = iszero(null_count) ? nomissing(T) : T

        series = new{T}(ptr, null_count, len)

        finalizer(polars_series_destroy, series)
    end
end

Base.unsafe_convert(::Type{Ptr{polars_series_t}}, series::Series) = series.ptr

Base.size(series::Series) = (series.length,)
Base.eltype(::Series{T}) where {T} = T

function Base.getindex(series::Series{MT}, index) where {MT<:Union{MaybeMissing{Integer},MaybeMissing{AbstractFloat}}}
    index = index - 1

    if series.null_count > 0 && polars_series_is_null(series, index)
        return missing
    end

    T = nomissing(MT)
    out = Ref{T}()

    letter = T <: AbstractFloat ? "f" :
             T <: Signed ? "i" : "u"
    name = T == Bool ? :polars_series_get_bool : Symbol("polars_series_get_", letter, 8sizeof(T))
    f = getproperty(API, name)

    err = f(series, index, out)
    polars_error(err)
    out[]
end

function Base.getindex(series::Series{MT}, index) where {MT<:Union{MaybeMissing{Series},MaybeMissing{String},MaybeMissing{NamedTuple},MaybeMissing{Vector{UInt8}}}}
    index = index - 1

    if series.null_count > 0 && polars_series_is_null(series, index)
        return missing
    end

    T = nomissing(MT)

    value_at_index = Value{T}(polars_series_get(series, index), series)

    load_value(value_at_index)
end

# function Series(name, values)
#     ptr = Ref{Ptr{polars_series_t}}()
# 
#     err = polars_series_new(name, length(name), values, length(values), ptr)
#     polars_error(err)
# 
#     Series{UInt32}(ptr[])
# end

"""
    name(series::Series)::String

Returns the name of this polars series.
"""
function name(series)
    ptr = Ref{Ptr{UInt8}}()
    len = polars_series_name(series, ptr)
    unsafe_string(ptr[], len)
end
