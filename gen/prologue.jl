const libpolars_local = joinpath(@__DIR__, "../c-polars/target/debug/libpolars.so")
@static if isfile(libpolars_local)
    const libpolars = libpolars_local
end
