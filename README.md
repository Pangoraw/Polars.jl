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

julia> select(gbagg,
           col("name_right") |> alias("nation_name"),
           col("customer_names"),
           col("acctbal"),
        ) |> collect
shape: (25, 3)
┌────────────────┬───────────────────────────────────┬─────────────┐
│ nation_name    ┆ customer_names                    ┆ acctbal     │
│ ---            ┆ ---                               ┆ ---         │
│ str            ┆ list[str]                         ┆ f64         │
╞════════════════╪═══════════════════════════════════╪═════════════╡
│ japan          ┆ ["Customer#000000025", "Customer… ┆ 4522.271135 │
│ egypt          ┆ ["Customer#000000004", "Customer… ┆ 4520.492752 │
│ united states  ┆ ["Customer#000000117", "Customer… ┆ 4565.65249  │
│ india          ┆ ["Customer#000000009", "Customer… ┆ 4517.316696 │
│ …              ┆ …                                 ┆ …           │
│ iraq           ┆ ["Customer#000000052", "Customer… ┆ 4514.438471 │
│ united kingdom ┆ ["Customer#000000011", "Customer… ┆ 4514.656468 │
│ canada         ┆ ["Customer#000000005", "Customer… ┆ 4489.259827 │
│ romania        ┆ ["Customer#000000043", "Customer… ┆ 4544.85113  │
└────────────────┴───────────────────────────────────┴─────────────┘
```

## Polars C-API

To build the polars c-api, run the following commands:

```
cd c-polars
cargo build # --release
```

This is mostly helpful for development to test C-API changes with the Julia version, do not forget to set `Polars.API.libpolars` to the right path by uncommenting the line in `src/API.jl`.
A header file is also included if one wants to use the API from C directly.
