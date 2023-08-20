# Polars.jl

Polars.jl is a thin wrapper for Julia around the dataframe manipulation library [polars](https://github.com/pola-rs/polars).

## Example

```julia
julia> using Polars

julia> customers = read_parquet("NONE_pandas_pyarrow_customer.parquet") |> lazy
Polars.LazyFrame(Ptr{Polars.polars_lazy_frame_t} @0x0000000003df3990)

julia> nations = read_parquet("NONE_pandas_pyarrow_nation.parquet") |> lazy
Polars.LazyFrame(Ptr{Polars.polars_lazy_frame_t} @0x00000000023fc6c0)

julia> customers_nations = join(customers, nations, [col("nation_key")], [col("nation_key")])
Polars.LazyFrame(Ptr{Polars.polars_lazy_frame_t} @0x0000000002104380)

julia> gb = groupby(customers_nations, [col("nation_key")])
Polars.LazyGroupBy(Ptr{Polars.polars_lazy_group_by_t} @0x0000000003d3dc90)

julia> gbagg = agg(gb, alias(col("name"), "customer_names"), col("name_right"), mean(col("acctbal")))
Polars.LazyFrame(Ptr{Polars.polars_lazy_frame_t} @0x0000000003cc78c0)

julia> select(gbagg,
           alias(col("name_right"), "nation_name") |> Lists.first,
           col("customer_names"),
           col("acctbal"),
        ) |> collect
shape: (25, 3)
┌────────────────┬───────────────────────────────────┬─────────────┐
│ nation_name    ┆ customer_names                    ┆ acctbal     │
│ ---            ┆ ---                               ┆ ---         │
│ str            ┆ list[str]                         ┆ f64         │
╞════════════════╪═══════════════════════════════════╪═════════════╡
│ INDIA          ┆ ["Customer#000000009", "Customer… ┆ 4517.316696 │
│ EGYPT          ┆ ["Customer#000000004", "Customer… ┆ 4520.492752 │
│ MOZAMBIQUE     ┆ ["Customer#000000044", "Customer… ┆ 4523.419223 │
│ JAPAN          ┆ ["Customer#000000025", "Customer… ┆ 4522.271135 │
│ …              ┆ …                                 ┆ …           │
│ MOROCCO        ┆ ["Customer#000000001", "Customer… ┆ 4496.793138 │
│ ROMANIA        ┆ ["Customer#000000043", "Customer… ┆ 4544.85113  │
│ UNITED KINGDOM ┆ ["Customer#000000011", "Customer… ┆ 4514.656468 │
│ CANADA         ┆ ["Customer#000000005", "Customer… ┆ 4489.259827 │
└────────────────┴───────────────────────────────────┴─────────────┘
```

## Polars C-API

To build the polars c-api, run the following commands (rustc 1.73.0-nightly is currently needed):

```
cd c-polars
cargo build # --release
```

A header file is also included if one wants to use the API from C directly.
