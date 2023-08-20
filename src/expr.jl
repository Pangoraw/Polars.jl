mutable struct Expr
    ptr::Ptr{polars_expr_t}

    Expr(ptr) =
        finalizer(expr -> @ccall(libpolars.polars_expr_destroy(expr::Ptr{polars_expr_t})::Cvoid), new(ptr))
end

Base.unsafe_convert(::Type{Ptr{polars_expr_t}}, expr::Expr) = expr.ptr

Base.promote_rule(::Type{Expr}, ::Type{UInt32}) = Expr
Base.promote_rule(::Type{Expr}, ::Type{UInt64}) = Expr
Base.promote_rule(::Type{Expr}, ::Type{Int32}) = Expr
Base.promote_rule(::Type{Expr}, ::Type{Int64}) = Expr

# TODO: improve this:
Base.convert(::Type{Expr}, p::Pair{Expr,Symbol}) = alias(p[1], string(p[2])::String)
Base.convert(::Type{Expr}, p::Pair{Symbol,<:Any}) = convert(Expr, col(string(p[1])) => p[2])
Base.convert(::Type{Expr}, p::Pair{Expr,<:Any}) = p[2](p[1])
Base.convert(::Type{Expr}, p::Pair{Expr,Pair{T,Symbol}}) where {T} = alias(p[2][1](p[1]), string(p[2][2]))

Base.convert(::Type{Expr}, ::Colon) = col("*")
function Base.convert(::Type{Expr}, v::Int32)
    out = @ccall libpolars.polars_expr_literal_i32(v::Int32)::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, v::Int64)
    out = @ccall libpolars.polars_expr_literal_i64(v::Int64)::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, v::UInt32)
    out = @ccall libpolars.polars_expr_literal_u32(v::UInt32)::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, v::UInt64)
    out = @ccall libpolars.polars_expr_literal_u64(v::UInt64)::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, v::Bool)
    out = @ccall libpolars.polars_expr_literal_bool(v::Bool)::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, ::Nothing)
    out = @ccall libpolars.polars_expr_literal_null()::Ptr{polars_expr_t}
    Expr(out)
end
function Base.convert(::Type{Expr}, s::String)
    out = Ref{Ptr{polars_expr_t}}()
    err = @ccall libpolars.polars_expr_literal_utf8(
        s::Ptr{Cchar},
        length(s)::Csize_t,
        out::Ptr{Ptr{polars_expr_t}},
    )::Ptr{polars_error_t}
    polars_error(err)
    Expr(out[])
end

Base.:+(a, b::Expr) = add(promote(a, b)...)
Base.:-(a, b::Expr) = sub(promote(a, b)...)
Base.:*(a, b::Expr) = mul(promote(a, b)...)
Base.:/(a, b::Expr) = div(promote(a, b)...)
Base.isequal(a, b::Expr) = eq(promote(a, b)...)
Base.isless(a, b::Expr) = Base.lt(promote(a, b)...)

Base.:+(a::Expr, b) = add(promote(a, b)...)
Base.:-(a::Expr, b) = sub(promote(a, b)...)
Base.:*(a::Expr, b) = mul(promote(a, b)...)
Base.:/(a::Expr, b) = div(promote(a, b)...)
Base.isequal(a::Expr, b) = eq(promote(a, b)...)
Base.isless(a::Expr, b) = Base.lt(promote(a, b)...)

function col(name)
    expr = Ref{Ptr{polars_expr_t}}()
    err = @ccall libpolars.polars_expr_col(
        name::Ptr{Cchar}, length(name)::Cuint,
        expr::Ptr{Ptr{polars_expr_t}},
    )::Ptr{polars_error_t}
    polars_error(err)
    return Expr(expr[])
end

function alias(expr, alias)
    out = Ref{Ptr{polars_expr_t}}()
    err = @ccall libpolars.polars_expr_alias(
        expr::Ptr{polars_expr_t},
        alias::Ptr{Cchar},
        length(alias)::Csize_t,
        out::Ptr{Ptr{polars_expr_t}},
    )::Ptr{polars_error_t}
    polars_error(err)
    return Expr(out[])
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
                out = @ccall libpolars.$(cname)(a::Ptr{polars_expr_t}, b::Ptr{polars_expr_t})::Ptr{polars_expr_t}
                Expr(out)
            end
        else
            push!(sig.args, Base.Expr(:(::), :expr, :Expr))
            body = quote
                out = @ccall libpolars.$(cname)(expr::Ptr{polars_expr_t})::Ptr{polars_expr_t}
                Expr(out)
            end
        end
        push!(out.args, Base.Expr(:function, sig, body))
        # Export Expr symbols
        if fname isa Symbol # && __module__ != Polars
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
    gen_impl_expr!(polars_expr_count, Expr::count)
    gen_impl_expr!(polars_expr_first, Expr::first)
    gen_impl_expr!(polars_expr_last, Expr::last)

    gen_impl_expr!(polars_expr_not, Expr::not)
    gen_impl_expr!(polars_expr_is_finite, Expr::is_finite)
    gen_impl_expr!(polars_expr_is_infinite, Expr::is_infinite)
    gen_impl_expr!(polars_expr_is_nan, Expr::is_nan)
    gen_impl_expr!(polars_expr_is_null, Expr::is_null)
    gen_impl_expr!(polars_expr_is_not_null, Expr::is_not_null)
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
end

module Lists
    using ..Polars: @generate_expr_fns, polars_expr_t, libpolars, Expr

    @generate_expr_fns begin
        gen_impl_expr_list!(polars_expr_list_lengths, ListNameSpace::lengths);
        gen_impl_expr_list!(polars_expr_list_max, ListNameSpace::max);
        gen_impl_expr_list!(polars_expr_list_min, ListNameSpace::min);
        gen_impl_expr_list!(polars_expr_list_arg_max, ListNameSpace::arg_max);
        gen_impl_expr_list!(polars_expr_list_arg_min, ListNameSpace::arg_min);
        gen_impl_expr_list!(polars_expr_list_sum, ListNameSpace::sum);
        gen_impl_expr_list!(polars_expr_list_mean, ListNameSpace::mean);
        gen_impl_expr_list!(polars_expr_list_reverse, ListNameSpace::reverse);
        gen_impl_expr_list!(polars_expr_list_unique, ListNameSpace::unique);
        gen_impl_expr_list!(polars_expr_list_unique_stable, ListNameSpace::unique_stable);
        gen_impl_expr_list!(polars_expr_list_first, ListNameSpace::first);
        gen_impl_expr_list!(polars_expr_list_last, ListNameSpace::last);

        gen_impl_expr_binary_list!(polars_expr_list_get, ListNameSpace::get);
        gen_impl_expr_binary_list!(polars_expr_list_head, ListNameSpace::head);
        gen_impl_expr_binary_list!(polars_expr_list_contains, ListNameSpace::contains);
    end
end

module Strings
    using ..Polars: @generate_expr_fns, polars_expr_t, libpolars, Expr

    @generate_expr_fns begin
        gen_impl_expr_str!(polars_expr_str_to_uppercase, StringNameSpace::uppercase);
        gen_impl_expr_str!(polars_expr_str_to_lowercase, StringNameSpace::lowercase);
        gen_impl_expr_str!(polars_expr_str_to_titlecase, StringNameSpace::titlecase);
        gen_impl_expr_str!(polars_expr_str_n_chars, StringNameSpace::n_chars);
        gen_impl_expr_str!(polars_expr_str_lengths, StringNameSpace::lengths);
        gen_impl_expr_str!(polars_expr_str_explode, StringNameSpace::explode);

        gen_impl_expr_binary_str!(polars_expr_str_starts_with, StringNameSpace::starts_with);
        gen_impl_expr_binary_str!(polars_expr_str_ends_with, StringNameSpace::ends_with);
        gen_impl_expr_binary_str!(
            polars_expr_str_contains_literal,
            StringNameSpace::contains_literal
        );
    end
end

export col, alias
