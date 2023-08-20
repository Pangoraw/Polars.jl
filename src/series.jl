struct Series{T} <: AbstractVector{T}
    ptr::Ptr{polars_series_t}
end

Base.unsafe_convert(::Type{Ptr{polars_series_t}}, series::Series) = series.ptr

Base.size(series::Series) = ((@ccall libpolars.polars_series_length(series::Ptr{polars_series_t})::Cuint),)
function Base.getindex(series::Series{UInt32}, index)
    out = Ref{UInt32}()
    index = index - 1
    err = @ccall libpolars.polars_series_get_u32(
        series::Ptr{polars_series_t},
        index::Cuint,
        out::Ptr{UInt32},
    )::Ptr{polars_error_t}
    polars_error(err)
    out[]
end

function Series(name, values)
    ptr = Ref{Ptr{polars_series_t}}()

    err = @ccall libpolars.polars_series_new(
        name::Ptr{UInt8}, length(name)::UInt,
        values::Ptr{UInt32}, length(values)::UInt,
        ptr::Ptr{Ptr{polars_series_t}}
    )::Ptr{polars_error_t}
    polars_error(err)

    Series{UInt32}(ptr[])
end

function name(series)
    ptr = Ref{Ptr{Cchar}}()
    len = @ccall libpolars.polars_series_name(series::Ptr{polars_series_t}, ptr::Ptr{Ptr{Cchar}})::Cuint
    unsafe_string(ptr[], len)
end
