import Pkg
Pkg.instantiate()

using Clang.Generators
using Clang.LibClang.Clang_jll

cd(@__DIR__)

include_dir = joinpath(@__DIR__, "../c-polars/include/")

# wrapper generator options
options = load_options(joinpath(@__DIR__, "generator.toml"))

# add compiler flags, e.g. "-DXXXXXXXXX"
args = get_default_args()
push!(args, "-I$include_dir")

headers = [joinpath(include_dir, "polars.h")]

# create context
ctx = create_context(headers, args, options)

# run generator
build!(ctx)
