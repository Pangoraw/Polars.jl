# Polars.jl

Polars.jl is a thin wrapper for Julia around the dataframe manipulation library [polars](https://github.com/pola-rs/polars).

## Example

```julia
julia> using Polars

julia> customers = read_parquet("NONE_pandas_pyarrow_customer.parquet") |> lazy;

julia> nations = read_parquet("NONE_pandas_pyarrow_nation.parquet") |> lazy;

julia> customers_nations = join(customers, nations, [col("nation_key")], [col("nation_key")]);

julia> gb = groupby(customers_nations, [col("nation_key")]);

julia> gbagg = agg(gb,
           alias(col("name"), "customer_names"),
           col("name_right") |> first |> Strings.titlecase,
           mean(col("acctbal"))
       );

julia> select(gbagg,
           alias(col("name_right"), "nation_name"),
           col("customer_names"),
           col("acctbal"),
        ) |> collect
shape: (25, 3)
┌────────────────┬───────────────────────────────────┬─────────────┐
│ nation_name    ┆ customer_names                    ┆ acctbal     │
│ ---            ┆ ---                               ┆ ---         │
│ str            ┆ list[str]                         ┆ f64         │
╞════════════════╪═══════════════════════════════════╪═════════════╡
│ Japan          ┆ ["Customer#000000025", "Customer… ┆ 4522.271135 │
│ Egypt          ┆ ["Customer#000000004", "Customer… ┆ 4520.492752 │
│ United States  ┆ ["Customer#000000117", "Customer… ┆ 4565.65249  │
│ India          ┆ ["Customer#000000009", "Customer… ┆ 4517.316696 │
│ …              ┆ …                                 ┆ …           │
│ Iraq           ┆ ["Customer#000000052", "Customer… ┆ 4514.438471 │
│ United Kingdom ┆ ["Customer#000000011", "Customer… ┆ 4514.656468 │
│ Canada         ┆ ["Customer#000000005", "Customer… ┆ 4489.259827 │
│ Romania        ┆ ["Customer#000000043", "Customer… ┆ 4544.85113  │
└────────────────┴───────────────────────────────────┴─────────────┘
```

## Polars C-API

To build the polars c-api, run the following commands (rustc 1.73.0-nightly is currently needed):

```
cd c-polars
cargo build # --release
```

A header file is also included if one wants to use the API from C directly.
