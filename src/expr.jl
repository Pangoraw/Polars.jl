"""
    Expr

Internal structure representing a value in a Polars expression.
This should not be constructed directly but rather use helper functions
such as [`col`](@ref).
"""
mutable struct Expr <: Number
                    #  ↑
                    #  this is needed to use type promotion
    ptr::Ptr{polars_expr_t}

    Expr(ptr) = finalizer(polars_expr_destroy, new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_expr_t}}, expr::Expr) = expr.ptr

Base.promote_rule(::Type{Expr}, ::Type{T}) where {T<:PhysicalDType} = Expr

Base.convert(::Type{Expr}, ::Colon) = col("*")
function Base.convert(::Type{Expr}, v::Int32)
    out = polars_expr_literal_i32(v)
    Expr(out)
end
function Base.convert(::Type{Expr}, v::Int64)
    out = polars_expr_literal_i64(v)
    Expr(out)
end
function Base.convert(::Type{Expr}, v::UInt32)
    out = polars_expr_literal_u32(v)
    Expr(out)
end
function Base.convert(::Type{Expr}, v::UInt64)
    out = polars_expr_literal_u64(v)
    Expr(out)
end
function Base.convert(::Type{Expr}, v::Bool)
    out = polars_expr_literal_bool(v)
    Expr(out)
end
function Base.convert(::Type{Expr}, f::Float32)
    out = polars_expr_literal_f32(f)
    Expr(out)
end
function Base.convert(::Type{Expr}, f::Float64)
    out = polars_expr_literal_f64(f)
    Expr(out)
end
function Base.convert(::Type{Expr}, ::Missing)
    out = polars_expr_literal_null()
    Expr(out)
end
function Base.convert(::Type{Expr}, s::String)
    out = Ref{Ptr{polars_expr_t}}()
    err = polars_expr_literal_utf8(s, sizeof(s), out)
    polars_error(err)
    Expr(out[])
end

Base.:(==)(a::Expr, b::Expr) = eq(a, b)
Base.isequal(a::Expr, b::Expr) = eq(a, b)
Base.isless(a::Expr, b::Expr) = Base.lt(a, b)
Base.isless(a::Expr, b) = isless(promote(a,b)...)
Base.isless(a, b::Expr) = isless(promote(a,b)...)
Base.isequal(a, b::Expr) = eq(promote(a, b)...)
Base.isequal(a::Expr, b) = eq(promote(a, b)...)

Base.:+(a::Expr, b::Expr) = add(a, b)
Base.:-(a::Expr, b::Expr) = sub(a, b)
Base.:*(a::Expr, b::Expr) = mul(a, b)
Base.:/(a::Expr, b::Expr) = div(a, b)
Base.:^(a::Expr, b::Expr) = pow(a, b)

Base.:&(a::Expr, b::Expr) = and(promote(a, b)...)
Base.:|(a::Expr, b::Expr) = or(promote(a, b)...)
Base.:&(a, b::Expr) = and(promote(a, b)...)
Base.:|(a, b::Expr) = or(promote(a, b)...)
Base.:&(a::Expr, b) = and(promote(a, b)...)
Base.:|(a::Expr, b) = or(promote(a, b)...)

"""
    col(name::String)::Polars.Expr

Returns an expression referencing a column in a dataframe. The special
column name `"*"` will select all columns in the dataframe.
"""
function col(name)
    expr = Ref{Ptr{polars_expr_t}}()
    err = polars_expr_col(name, sizeof(name), expr)
    polars_error(err)
    return Expr(expr[])
end

"""
    alias(expr::Polars.Expr, name::String)::Polars.Expr
    alias(alias::String)::Base.Fix2{typeof(alias), String}

Renames the result of this expression to a new name.
"""
function alias(expr, alias)
    out = Ref{Ptr{polars_expr_t}}()
    err = polars_expr_alias(expr, alias, sizeof(alias), out)
    polars_error(err)
    return Expr(out[])
end
alias(new_name) = Base.Fix2(alias, new_name)

"""
    prefix(expr::Polars.Expr, prefix::String)::Polars.Expr
    prefix(prefix::String)::Base.Fix2{typeof(prefix), String}

Adds a prefix to the name of the resulting expression.
"""
function prefix(expr, pref)
    out = Ref{Ptr{polars_expr_t}}()
    err = polars_expr_prefix(expr, pref, sizeof(pref), out)
    polars_error(err)
    return Expr(out[])
end
prefix(pref) = Base.Fix2(prefix, pref)

"""
    suffix(expr::Polars.Expr, suffix::String)::Polars.Expr
    suffix(suffix::String)::Base.Fix2{typeof(suffix), String}

Adds a suffix to the name of the resulting expression.
"""
function suffix(expr, suf)
    out = Ref{Ptr{polars_expr_t}}()
    err = polars_expr_suffix(expr, suf, sizeof(suf), out)
    polars_error(err)
    return Expr(out[])
end
suffix(suf) = Base.Fix2(suffix, suf)

"""
    lit(x)::Polars.Expr
Transforms a literal value as an expression which will broadcast when used with other
expressions.
"""
function lit(v)
    convert(Expr, v) 
end

Base.count() = Expr(polars_expr_count())

"""
    cast(expr::Polars.Expr, dtype::Type)::Polars.Expr
    cast(dtype::Type)::Base.Fix2{typeof(cast), ::Type}

Casts the series represented by the expression with provided the datatype.
"""
function cast(expr, dtype)
    value_type = if dtype == Missing
        PolarsValueTypeNull
    elseif dtype == Bool
        PolarsValueTypeBoolean
    elseif dtype == UInt8
        PolarsValueTypeUInt8
    elseif dtype == UInt16
        PolarsValueTypeUInt16
    elseif dtype == UInt32
        PolarsValueTypeUInt32
    elseif dtype == UInt64
        PolarsValueTypeUInt64
    elseif dtype == Int8
        PolarsValueTypeInt8
    elseif dtype == Int16
        PolarsValueTypeInt16
    elseif dtype == Int32
        PolarsValueTypeInt32
    elseif dtype == Int64
        PolarsValueTypeInt64
    elseif dtype == Float32
        PolarsValueTypeFloat32
    elseif dtype == Float64
        PolarsValueTypeFloat64
    elseif dtype == String
        PolarsValueTypeUtf8
    else
        error("could not cast to type $dtype")
    end

    casted = API.polars_expr_cast(expr, value_type)
    Expr(casted)
end
cast(dtype) = Base.Fix2(cast, dtype)

"""
    var(expr::Polars.Expr, ddof::Integer=1)::Polars.Expr

Refer to [the polars documentation](https://docs.rs/polars/latest/polars/prelude/enum.Expr.html#method.var).
"""
function var(expr, ddof=1)
    Expr(polars_expr_var(expr, ddof))
end

"""
    std(expr::Polars.Expr, ddof::Integer=1)::Polars.Expr

Refer to [the polars documentation](https://docs.rs/polars/latest/polars/prelude/enum.Expr.html#method.std).
"""
function std(expr, ddof=1)
    Expr(polars_expr_std(expr, ddof))
end

macro generate_expr_fns(ex)
    @assert ex.head === :block
    out = Base.Expr(:block)
    for call in ex.args
        call isa Base.Expr || continue
        cname = call.args[2]
        fname = last(last(call.args).args)
        if __module__ == Polars && isdefined(Base, fname)
            fname = Base.Expr(:(.), :Base, QuoteNode(fname))
        end
        sig = Base.Expr(:call, fname)
        gen_name = string(first(call.args))
        @assert occursin("gen", gen_name)
        if occursin("binary", gen_name)
            push!(sig.args, Base.Expr(:(::), :a, :Expr), Base.Expr(:(::), :b, :Expr))
            body = quote
                out = API.$(cname)(a, b)
                Expr(out)
            end
        else
            push!(sig.args, Base.Expr(:(::), :expr, :Expr))
            body = quote
                out = API.$(cname)(expr)
                Expr(out)
            end
        end
        push!(out.args, Base.Expr(:function, sig, body))
        # Export Expr symbols
        if fname isa Symbol # && __module__ != Polars
            namespace = string(first(last(call.args).args))
            namespace_type = namespace == "Expr" ? "enum" : "struct"
            rust_doc_url = "https://docs.rs/polars/latest/polars/prelude/$(namespace_type).$(namespace).html#method.$fname"
            string_sig = replace(string(sig), "Expr" => "Polars.Expr")
            docstring = """
                $(string_sig)::Polars.Expr

            Refer to [the polars documentation]($rust_doc_url).
            """
            push!(out.args, quote
                Docs.@doc $docstring $(QuoteNode(fname))
            end)
            push!(out.args, :(export $fname))
        end
    end
    esc(out)
end

# We just copy the rust code here and generate functions on the fly.
@generate_expr_fns begin
    gen_impl_expr!(polars_expr_keep_name, Expr::keep_name)

    gen_impl_expr!(polars_expr_sum, Expr::sum)
    gen_impl_expr!(polars_expr_product, Expr::product)
    gen_impl_expr!(polars_expr_mean, Expr::mean)
    gen_impl_expr!(polars_expr_median, Expr::median)
    gen_impl_expr!(polars_expr_min, Expr::min)
    gen_impl_expr!(polars_expr_max, Expr::max)
    gen_impl_expr!(polars_expr_arg_min, Expr::arg_min)
    gen_impl_expr!(polars_expr_arg_max, Expr::arg_max)
    gen_impl_expr!(polars_expr_nan_min, Expr::nan_min)
    gen_impl_expr!(polars_expr_nan_max, Expr::nan_max)

    gen_impl_expr!(polars_expr_floor, Expr::floor)
    gen_impl_expr!(polars_expr_ceil, Expr::ceil)
    gen_impl_expr!(polars_expr_abs, Expr::abs)
    gen_impl_expr!(polars_expr_cos, Expr::cos)
    gen_impl_expr!(polars_expr_sin, Expr::sin)
    gen_impl_expr!(polars_expr_tan, Expr::tan)
    gen_impl_expr!(polars_expr_cosh, Expr::cosh)
    gen_impl_expr!(polars_expr_sinh, Expr::sinh)
    gen_impl_expr!(polars_expr_tanh, Expr::tanh)

    gen_impl_expr!(polars_expr_n_unique, Expr::n_unique)
    gen_impl_expr!(polars_expr_unique, Expr::unique)
    gen_impl_expr!(polars_expr_count_unary, Expr::count)
    gen_impl_expr!(polars_expr_first, Expr::first)
    gen_impl_expr!(polars_expr_last, Expr::last)

    gen_impl_expr!(polars_expr_not, Expr::not)
    gen_impl_expr!(polars_expr_is_finite, Expr::is_finite)
    gen_impl_expr!(polars_expr_is_infinite, Expr::is_infinite)
    gen_impl_expr!(polars_expr_is_nan, Expr::is_nan)
    gen_impl_expr!(polars_expr_is_null, Expr::is_null)
    gen_impl_expr!(polars_expr_is_not_null, Expr::is_not_null)
    gen_impl_expr!(polars_expr_null_count, Expr::null_count)
    gen_impl_expr!(polars_expr_drop_nans, Expr::drop_nans)
    gen_impl_expr!(polars_expr_drop_nulls, Expr::drop_nulls)

    gen_impl_expr!(polars_expr_implode, Expr::implode)
    gen_impl_expr!(polars_expr_flatten, Expr::flatten)
    gen_impl_expr!(polars_expr_reverse, Expr::reverse)

    gen_impl_expr_binary!(polars_expr_eq, Expr::eq)
    gen_impl_expr_binary!(polars_expr_lt, Expr::lt)
    gen_impl_expr_binary!(polars_expr_gt, Expr::gt)
    gen_impl_expr_binary!(polars_expr_or, Expr::or)
    gen_impl_expr_binary!(polars_expr_xor, Expr::xor)
    gen_impl_expr_binary!(polars_expr_and, Expr::and)

    gen_impl_expr_binary!(polars_expr_pow, Expr::pow)
    gen_impl_expr_binary!(polars_expr_add, Expr::add)
    gen_impl_expr_binary!(polars_expr_sub, Expr::sub)
    gen_impl_expr_binary!(polars_expr_mul, Expr::mul)
    gen_impl_expr_binary!(polars_expr_div, Expr::div)

    gen_impl_expr_binary!(polars_expr_fill_null, Expr::fill_null)
    gen_impl_expr_binary!(polars_expr_fill_nan, Expr::fill_nan)
end

module Lists
using ..Polars: @generate_expr_fns, API, polars_expr_t, Expr

@generate_expr_fns begin
    gen_impl_expr_list!(polars_expr_list_lengths, ListNameSpace::lengths)
    gen_impl_expr_list!(polars_expr_list_max, ListNameSpace::max)
    gen_impl_expr_list!(polars_expr_list_min, ListNameSpace::min)
    gen_impl_expr_list!(polars_expr_list_arg_max, ListNameSpace::arg_max)
    gen_impl_expr_list!(polars_expr_list_arg_min, ListNameSpace::arg_min)
    gen_impl_expr_list!(polars_expr_list_sum, ListNameSpace::sum)
    gen_impl_expr_list!(polars_expr_list_mean, ListNameSpace::mean)
    gen_impl_expr_list!(polars_expr_list_reverse, ListNameSpace::reverse)
    gen_impl_expr_list!(polars_expr_list_unique, ListNameSpace::unique)
    gen_impl_expr_list!(polars_expr_list_unique_stable, ListNameSpace::unique_stable)
    gen_impl_expr_list!(polars_expr_list_first, ListNameSpace::first)
    gen_impl_expr_list!(polars_expr_list_last, ListNameSpace::last)

    gen_impl_expr_binary_list!(polars_expr_list_get, ListNameSpace::get)
    gen_impl_expr_binary_list!(polars_expr_list_head, ListNameSpace::head)
    gen_impl_expr_binary_list!(polars_expr_list_contains, ListNameSpace::contains)
end
end # module Lists

module Strings
using ..Polars: @generate_expr_fns, API, polars_expr_t, Expr

@generate_expr_fns begin
    gen_impl_expr_str!(polars_expr_str_to_uppercase, StringNameSpace::uppercase)
    gen_impl_expr_str!(polars_expr_str_to_lowercase, StringNameSpace::lowercase)
    gen_impl_expr_str!(polars_expr_str_to_titlecase, StringNameSpace::titlecase)
    gen_impl_expr_str!(polars_expr_str_n_chars, StringNameSpace::n_chars)
    gen_impl_expr_str!(polars_expr_str_lengths, StringNameSpace::lengths)
    gen_impl_expr_str!(polars_expr_str_explode, StringNameSpace::explode)

    gen_impl_expr_binary_str!(polars_expr_str_starts_with, StringNameSpace::starts_with)
    gen_impl_expr_binary_str!(polars_expr_str_ends_with, StringNameSpace::ends_with)
    gen_impl_expr_binary_str!(
        polars_expr_str_contains_literal,
        StringNameSpace::contains_literal
    )
end
end # module Strings

module Structs
using ..Polars: Expr, API

"""
    field_by_name(expr::Polars.Expr, name::String)::Polars.Expr
    field_by_name(name::String)::Base.Fix2{typeof(field_by_name), String}

Returns a new series corresponding to values of the selected field.
"""
function field_by_name(expr, name)
    field = API.polars_expr_struct_field_by_name(expr, name, sizeof(name))
    Expr(field)
end
field_by_name(name) = Base.Fix2(field_by_name, name)

"""
    field_by_index(expr::Polars.Expr, index::Integer)::Polars.Expr
    field_by_index(index::Integer)::Base.Fix2{typeof(field_by_index), Integer}

Returns a new series corresponding to values of the selected field.
"""
function field_by_index(expr, fieldidx)
    field = API.polars_expr_struct_field_by_index(expr, fieldidx)
    Expr(field)
end
field_by_index(fieldidx) = Base.Fix2(field_by_index, fieldidx)

"""
    rename_fields(expr::Polars.Expr, new_names::Vector{String})::Polars.Expr
    rename_fields(new_names::Vector{String})::Base.Fix2{typeof(rename_fields), Vector{String}}

Renames the fields of the struct series with the provided new names.
"""
function rename_fields(expr, new_names)
    new_names = convert(Vector{String}, new_names)
    new_struct = API.polars_expr_struct_rename_fields(expr, new_names, sizeof.(new_names), length(new_names))
    @assert new_struct != C_NULL "failed to rename fields"
    Expr(new_struct)
end
rename_fields(new_names) = Base.Fix2(rename_fields, new_names)

export field_by_name, field_by_index, rename_fields

end # module Structs

export col, alias, prefix, suffix, lit, cast,
       var, std,
       Lists, Strings, Structs
