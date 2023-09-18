# Polars.jl

[![](https://img.shields.io/badge/docs-dev-blue.svg)](https://pangoraw.github.io/Polars.jl/docs)

Polars.jl is a thin wrapper for Julia around the dataframe manipulation library [polars](https://github.com/pola-rs/polars).

## Example

```julia
julia> using Polars

julia> customers = read_parquet("NONE_pandas_pyarrow_customer.parquet") |> lazy;

julia> nations = read_parquet("NONE_pandas_pyarrow_nation.parquet") |> lazy;

julia> customers_nations = innerjoin(customers, nations, col("nation_key"));

julia> gb = groupby(customers_nations, [col("nation_key")]);

julia> gbagg = agg(gb,
           col("name") |> alias("customer_names"),
           col("name_right") |> first |> Strings.lowercase,
           col("acctbal") |> mean,
       );

julia> gbagg_sorted = sort(gbagg, "name_right");

julia> select(gbagg_sorted,
           col("name_right") |> alias("nation_name"),
           col("customer_names"),
           col("acctbal"),
        ) |> collect
25×3 DataFrame
 nation_name  customer_names                    acctbal 
 String       Series{Union{Missing, String}}    Float64 
────────────────────────────────────────────────────────
     algeria  ["Customer#000000029", "Custome…   4442.7
   argentina  ["Customer#000000003", "Custome…   4485.0
      brazil  ["Customer#000000017", "Custome…  4471.02
      canada  ["Customer#000000005", "Custome…  4489.26
       china  ["Customer#000000007", "Custome…  4438.95
       egypt  ["Customer#000000004", "Custome…  4520.49
    ethiopia  ["Customer#000000010", "Custome…  4467.37
      france  ["Customer#000000018", "Custome…  4436.01
      ⋮                      ⋮                     ⋮
                                         17 rows omitted
```

## Polars C-API

To build the polars c-api, run the following commands:

```
cd c-polars
cargo build # --release
```

This is mostly helpful for development to test C-API changes with the Julia version.
[A header file]() is also included if one wants to use the API from C directly.
