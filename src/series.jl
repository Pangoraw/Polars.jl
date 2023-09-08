mutable struct Series{T} <: AbstractVector{T}
    ptr::Ptr{polars_series_t}

    function Series(ptr)
        @assert ptr != C_NULL

        T = polars_series_type(ptr) |> julia_wrapper
        T == Nothing && error("cannot yet make series of this type")
        s = new{T}(ptr)

        finalizer(polars_series_destroy, s)
    end
end

Base.unsafe_convert(::Type{Ptr{polars_series_t}}, series::Series) = series.ptr

Base.size(series::Series) = (polars_series_length(series),)

function Base.getindex(series::Series{T}, index) where {T<:Union{Integer,AbstractFloat}}
    @assert parentmodule(T) == Core "type $T is not supported"

    out = Ref{T}()
    index = index - 1

    letter = T <: AbstractFloat ? "f" :
             T <: Signed ? "i" : "u"
    name = Symbol("polars_series_get_", letter, 8sizeof(T))
    f = getproperty(API, name)

    err = f(series, index, out)
    polars_error(err)
    out[]
end

function Series(name, values)
    ptr = Ref{Ptr{polars_series_t}}()

    err = polars_series_new(name, length(name), values, length(values), ptr)
    polars_error(err)

    Series{UInt32}(ptr[])
end

function name(series)
    ptr = Ref{Ptr{Cchar}}()
    len = polars_series_name(series, ptr)
    unsafe_string(ptr[], len)
end
