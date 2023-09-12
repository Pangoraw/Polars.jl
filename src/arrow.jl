# https://arrow.apache.org/docs/format/CDataInterface.html#
# https://arrow.apache.org/docs/format/Columnar.html#format-columnar

using .API:
    ArrowSchema as CArrowSchema,
    ArrowArray as CArrowArray

function parse_format(schema)
    fmt = unsafe_string(schema.format)

    fmt == "n" && return MaybeMissing{Nothing}
    fmt == "b" && return MaybeMissing{Bool}
    fmt == "c" && return MaybeMissing{Int8}
    fmt == "C" && return MaybeMissing{UInt8}
    fmt == "s" && return MaybeMissing{Int16}
    fmt == "S" && return MaybeMissing{UInt16}
    fmt == "i" && return MaybeMissing{Int32}
    fmt == "I" && return MaybeMissing{UInt32}
    fmt == "l" && return MaybeMissing{Int64}
    fmt == "L" && return MaybeMissing{UInt64}
    fmt == "e" && return MaybeMissing{Float16}
    fmt == "f" && return MaybeMissing{Float32}
    fmt == "g" && return MaybeMissing{Float64}
    fmt == "U" && return MaybeMissing{String}
    fmt == "u" && return MaybeMissing{String}
    fmt == "z" && return Vector{UInt8}
    fmt == "Z" && return Vector{UInt8}

    if fmt == "+s" # Struct type
        children = unsafe_wrap(
            Array,
            schema.children,
            schema.n_children,
        )
        names_types = map(children) do schema
            schema = unsafe_load(schema)
            (Symbol(unsafe_string(schema.name)),
                parse_format(schema))
        end
        names = Tuple(first.(names_types))
        types = Tuple{last.(names_types)...}
        return MaybeMissing{NamedTuple{names,types}}
    end

    # List but which are store as Series
    # NOTE: we may want to change this if the arrow implementation is
    # ....  is not specific to Polars.jl anymore.
    if fmt in ("+l", "+L")
        @assert schema.n_children == 1
        children = unsafe_load(schema.children) |> unsafe_load
        T = parse_format(children)
        return MaybeMissing{Series{T}}
    end

    if startswith(fmt, "+w") # Fixed size list 
        @assert schema.n_children
        children = unsafe_load(schema.children) |> unsafe_load
        T = parse_format(children)
        N = parse(Int, fmt[4:end])
        return MaybeMissing{NTuple{N,T}}
    end

    error("unknow schema format $fmt")
end

"""
    Internal API

!!! warning
    The schema should not be used afterwards.
"""
function load_series_schema(schema::CArrowSchema)
    res = unsafe_string(schema.name) => parse_format(schema)

    schema_ref = Ref(schema)
    @ccall $(schema.release)(schema_ref::Ptr{CArrowSchema})::Cvoid

    res
end

"""
    Internal API

!!! warning
    The schema should not be used afterwards.
"""
function load_dataframe_schema(schema::CArrowSchema)
    fmt = unsafe_string(schema.format)
    @assert fmt == "+s" "invalid polars schema"

    name = unsafe_string(schema.name)
    @assert name == "polars.dataframe" "invalid polars schema"

    NT = parse_format(schema)
    NT = nomissing(NT)
    @assert NT <: NamedTuple
    names, types = NT.parameters

    schema_ref = Ref(schema)
    @ccall $(schema.release)(schema_ref::Ptr{CArrowSchema})::Cvoid

    Tables.Schema(names, types)
end

"""
    ArrowSchema(; format, name, children=ArrowSchema[])

A Julia managed ArrowSchema valid according to the arrow C data interface.
"""
mutable struct ArrowSchema
    format::String
    name::String
    metadata::Union{Nothing,String}
    flags::Int64
    children::Vector{ArrowSchema}
    dictionary::Union{Nothing,ArrowSchema}

    children_pointers::Vector{Ptr{CArrowSchema}}
    carrow_schema::CArrowSchema
end

function release_schema!(schema)
    for child in schema.children
        delete!(LIVE_SCHEMAS, child)
    end
    delete!(LIVE_SCHEMAS, schema)
end

function base_release_schema(schema_ptr::Ptr{CArrowSchema})
    cschema = unsafe_load(schema_ptr)
    schema = unsafe_pointer_to_objref(Ptr{ArrowSchema}(cschema.private_data))
    release_schema!(schema)
    nothing
end

function set_private_data!(schema::ArrowSchema)
    base_release_ptr = @cfunction base_release_schema Cvoid (Ptr{CArrowSchema},)
    schema.carrow_schema = CArrowSchema(
        schema.carrow_schema.format,
        schema.carrow_schema.name,
        schema.carrow_schema.metadata,
        schema.carrow_schema.flags,
        schema.carrow_schema.n_children,
        schema.carrow_schema.children,
        schema.carrow_schema.dictionary,
        base_release_ptr,
        pointer_from_objref(schema),
    )
    @assert !haskey(LIVE_SCHEMAS, schema)
    LIVE_SCHEMAS[schema] = nothing
    nothing
end

function ArrowSchema(; format, name, metadata=nothing, flags=0, children=ArrowSchema[], dictionary=nothing)
    children_pointers = [
        Base.unsafe_convert(Ptr{CArrowSchema}, child)
        for child in children
    ]
    schema = ArrowSchema(
        format,
        name,
        metadata,
        flags,
        children,
        dictionary,
        children_pointers,
        CArrowSchema(
            Base.unsafe_convert(Cstring, format),
            Base.unsafe_convert(Cstring, name),
            isnothing(metadata) ? C_NULL : Base.unsafe_convert(Ptr{UInt8}, metadata),
            flags,
            length(children),
            pointer(children_pointers),
            isnothing(dictionary) ? C_NULL : throw("unsupported dictionary"),
            C_NULL,
            C_NULL,
        )
    )
    set_private_data!(schema)
    schema
end

function Base.unsafe_convert(::Type{Ptr{CArrowSchema}}, schema::ArrowSchema)
    Ptr{CArrowSchema}(
        Ptr{UInt8}(Base.pointer_from_objref(schema)) +
        fieldoffset(ArrowSchema, findfirst(==(:carrow_schema), fieldnames(ArrowSchema)))
    )
end

function format(T)
    if T <: Vector
        return "+l"
    end

    @assert !ismutabletype(T)
    if isstructtype(T)
        return "+s"
    end

    throw("cannot find a arrow format for type $T")
end
format(::Type{Nothing}) = "n"
format(::Type{Bool}) = "b"
format(::Type{Int8}) = "c"
format(::Type{UInt8}) = "C"
format(::Type{Int16}) = "s"
format(::Type{UInt16}) = "S"
format(::Type{Int32}) = "i"
format(::Type{UInt32}) = "I"
format(::Type{Int64}) = "l"
format(::Type{UInt64}) = "L"
format(::Type{Float16}) = "e"
format(::Type{Float32}) = "f"
format(::Type{Float64}) = "g"
format(::Type{Vector{UInt8}}) = "z"
format(::Type{Vector{<:Any}}) = "+l"
format(::Type{String}) = "u"

mutable struct ArrowArray
    length::Int64

    buffers::Vector{Union{Ptr,Vector}}
    buffer_ptrs::Vector{Ptr{UInt8}}

    children::Vector{ArrowArray}
    children_ptrs::Vector{Ptr{CArrowArray}}

    carrow_array::CArrowArray
end

function release_array!(array)
    for child in array.children
        delete!(LIVE_ARRAYS, child)
    end
    delete!(LIVE_ARRAYS, array)
end

function base_release_array(carray_ptr::Ptr{CArrowArray})
    carray = unsafe_load(carray_ptr)
    array = unsafe_pointer_to_objref(Ptr{ArrowArray}(carray.private_data))
    release_array!(array)

    nothing
end

"""
    set_private_data!(array::ArrowArray)

Makes the arrow array Julia managed.
"""
function set_private_data!(array::ArrowArray)
    base_release_ptr = @cfunction base_release_array Cvoid (Ptr{CArrowArray},)

    array.carrow_array = CArrowArray(
        array.carrow_array.length,
        array.carrow_array.null_count,
        array.carrow_array.offset,
        array.carrow_array.n_buffers,
        array.carrow_array.n_children,
        array.carrow_array.buffers,
        array.carrow_array.children,
        array.carrow_array.dictionary,
        base_release_ptr,
        pointer_from_objref(array),
    )
    @assert !haskey(LIVE_ARRAYS, array)

    LIVE_ARRAYS[array] = nothing
    nothing
end

function ArrowArray(buffers, children=[])
    l = length(buffers) >= 2 ?
        length(last(buffers)) :
        only(map(child -> child.length, children) |> unique)

    buffer_ptrs = [buffer isa Ptr ? Ptr{UInt8}(buffer) : Ptr{UInt8}(pointer(buffer))
                   for buffer in buffers]
    children_ptrs = [Base.unsafe_convert(Ptr{CArrowArray}, children) for children in children]

    array = ArrowArray(
        l,
        buffers,
        buffer_ptrs,
        children,
        children_ptrs,
        CArrowArray(
            l,
            0,
            0,
            length(buffers),
            length(children),
            pointer(buffer_ptrs),
            pointer(children_ptrs),
            C_NULL,
            C_NULL,
            C_NULL,
        )
    )
    set_private_data!(array)
    array
end

Base.cconvert(::Type{CArrowArray}, array::ArrowArray) = array
Base.unsafe_convert(::Type{CArrowArray}, array::ArrowArray) = array.carrow_array

function Base.unsafe_convert(::Type{Ptr{CArrowArray}}, array::ArrowArray)
    Ptr{CArrowArray}(
        Ptr{UInt8}(Base.pointer_from_objref(array)) +
        fieldoffset(ArrowArray, findfirst(==(:carrow_array), fieldnames(ArrowArray)))
    )
end

"Holds references to the live schemas whose ownership has been given through ffi."
const LIVE_SCHEMAS = IdDict{ArrowSchema,Nothing}()

"Holds references to the live arrays whose ownership has been given through ffi."
const LIVE_ARRAYS = IdDict{ArrowArray,Nothing}()

function arrowvector(v::Vector{T}) where {T<:PhysicalDType}
    @assert !any(ismissing, v)
    ArrowArray([C_NULL, v], [])
end

function arrowvector(v::Vector{MaybeMissing{T}}) where {T<:PhysicalDType}
    validity_map = ones(UInt8, div(length(v), 8))
    for i in eachindex(v)
        el = v[i]
        ismissing(el) && (validity_map[i%8] ^= 1)
    end
    ArrowArray([validity_map, v], [])
end

# Encodes the provided table to an ArrowArray
# this code should not fail as it can leak memory
# by populating LIVE_SCHEMAS or LIVE_ARRAYS with
# handles which are not given back to the caller
# in case of failure.
function arrowtable(table, table_name)
    tschema = Tables.schema(table)

    children = map(zip(tschema.names, tschema.types)) do (name, type)
        ArrowSchema(; format=format(type), name=string(name))
    end

    schema = ArrowSchema(; 
        format="+s",
        name=table_name,
        children,
    )

    array = ArrowArray([C_NULL], [
        arrowvector(t)
        for t in Tables.columns(table)
    ])

    array, schema
end
