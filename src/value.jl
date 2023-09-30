
"""
    Polars.Value{T}

Internal type which represents a reference to a value of type `T` in a series or as a field to
a struct.
"""
mutable struct Value{T}
    ptr::Ptr{polars_value_t}
    parent::Union{Series,Value}

    Value{T}(ptr, parent=nothing) where {T} =
        finalizer(polars_value_destroy, new{T}(ptr, parent))
end

Base.unsafe_convert(::Type{Ptr{polars_value_t}}, value::Value) = value.ptr

"""
    load_value(v::Value{T})::T

Materializes the polars value as a Julia value of type `T`.

!!! note
    This is an internal API.
"""
function load_value(value::Value{T}) where {T <: PhysicalDType}
    polars_value_type(value) == PolarsValueTypeNull && return missing

    letter = T <: AbstractFloat ? "f" :
             T <: Signed ? "i" : "u"
    name = T == Bool ? :polars_value_get_bool : Symbol("polars_value_get_", letter, 8sizeof(T))
    f = getproperty(API, name)

    out = Ref{T}()
    err = f(value, out)
    polars_error(err)

    out[]
end

function load_value(value::Value{String})
    polars_value_type(value) == PolarsValueTypeNull && return missing

    io = Ref(IOBuffer())
    callback = @cfunction(_write_callback, Cssize_t, (Any, Ptr{Cchar}, Cuint))

    err = polars_value_utf8_get(value, io, callback)
    polars_error(err)

    String(take!(io[]))
end

function load_value(value::Value{Vector{UInt8}})
    polars_value_type(value) == PolarsValueTypeNull && return missing

    io = Ref(IOBuffer())
    callback = @cfunction(_write_callback, Cssize_t, (Any, Ptr{Cchar}, Cuint))

    err = polars_value_binary_get(value, io, callback)
    polars_error(err)

    take!(io[])
end

function load_value(value::Value{S}) where {S <: Series}
    polars_value_type(value) == PolarsValueTypeNull && return missing

    out = Ref{Ptr{polars_series_t}}()

    err = polars_value_list_get(value, out)
    polars_error(err)

    Series(out[])
end

function load_value(value::Value{NT}) where {NT<:NamedTuple}
    polars_value_type(value) == PolarsValueTypeNull && return missing

    _, types = NT.parameters
    types = types.parameters

    field_values = map(enumerate(types)) do args
        field_index, T = args
        field_value_out = Ref{Ptr{polars_value_t}}()
        err = polars_value_struct_get(value, field_index - 1, field_value_out)
        polars_error(err)
        field_value = field_value_out[]

        # NOTE: Polars cannot figure the type of a single value whose type is null?
        if polars_value_type(field_value) == PolarsValueTypeUnknown
            return missing
        end

        T = nomissing(T)
        load_value(Value{T}(field_value, value))
    end

    NT(field_values)
end
