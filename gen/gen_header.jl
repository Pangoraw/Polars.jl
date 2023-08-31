# Think of it like Clang.jl but in reverse

const mappings = Dict(
    :Cuint => "unsigned",
    :Bool => "char",
    :Cvoid => "void",
    :Cint => "int",
    :UInt8 => "char",
    :UInt => "unsigned long",
    :UInt32 => "unsigned",
    :UInt64 => "unsigned long",
    :Int32 => "int",
    :Int64 => "long",
    :Cchar => "char",
    :Csize_t => "size_t",
    :Any => "void *",
)

function write_type(io::IO, expr)
    if Meta.isexpr(expr, :curly, 2) && expr.args[1] == :Ptr
        write_type(io, last(expr.args))
        print(io, '*')
        return
    elseif expr isa Symbol
        print(io, get(mappings, expr, string(expr)), " ")
    else
        error("cannot transcribe expr $expr to c type")
    end
end

function gen_header(io::IO, expr)
    @assert Meta.isexpr(expr, :macrocall)
    @assert first(expr.args) == Symbol("@ccall")

    call = last(expr.args)
    @assert Meta.isexpr(call, :(::), 2)
    call, ret = call.args

    name = first(call.args)
    @assert Meta.isexpr(name, :(.), 2)
    @assert first(name.args) == :libpolars
    name = last(name.args).value

    name isa Symbol || return

    write_type(io, ret)
    print(io, string(name), '(')

    isfirst = true
    for arg in @view call.args[begin+1:end]
        @assert Meta.isexpr(arg, :(::), 2)
        argname, type = arg.args

        if !isfirst
            print(io, ", ")
        else
            isfirst = false
        end

        write_type(io, type)
        if argname isa Symbol
            print(io, string(argname))
        elseif Meta.isexpr(argname, :call) && first(argname.args) == :length
            argname = string(last(argname.args)) * "len"
            print(io, argname)
        end
    end

    println(io, ");")
end

function rec_gen(io::IO, expr)
    if Meta.isexpr(expr, :struct) && all(ex -> ex isa LineNumberNode, expr.args[3].args)
        name = expr.args[2]
        println(io)
        println(io, "typedef struct ", string(name), " ", string(name), ";")
    elseif Meta.isexpr(expr, :macrocall) && first(expr.args) == Symbol("@ccall")
        println(io)
        gen_header(io, expr)
    elseif expr isa Expr
        foreach(arg -> rec_gen(io, arg), expr.args)
    end
end

function main()
    open("c-polars/include/polars.h", "w") do io
        println(io, """
        #ifndef POLARS_H
        #define POLARS_H

        #include <stdlib.h>""")
        for (root, _, files) in walkdir("src")
            for file in files
                @info file
                expr = Meta.parseall(read(joinpath(root, file), String))
                rec_gen(io, expr)
            end
        end
        println(io)
        print(io, "#endif // POLARS_H")
    end
end

main()
