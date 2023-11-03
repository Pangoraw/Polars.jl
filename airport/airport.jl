using Polars, CSV

# df = CSV.read("/home/pberg/Downloads/2007.csv", Polars.DataFrame; header=true, pool=true, stringtype=String)
# df = lazy(df)

"transforms categorical to int by casting to string"
to_int(c) = col(c) |> cast(String) |> cast(Int)

is_delay_prop = ((col("LateAircraftDelay") > 0) & (col("Delay") > 0)) |> alias("DelayProp")
let df = select(
        with_columns(df,
            fill_null(to_int("ArrDelay") |> alias("Delay"), lit(0)),
            to_int("LateAircraftDelay"),
        ),
        is_delay_prop,
        "Delay",
        "LateAircraftDelay",
        "Origin", "Dest"
    )

    gb = groupby(df, "Origin", "Dest")
    df = agg(gb,
        col("Delay") |> mean |> suffix(".mean"),
        col("LateAircraftDelay") |> mean |> suffix(".mean"),
        col("DelayProp") |> mean,
        count(),
    )

    df = filter(df, col("DelayProp") >= 0.1)
    df = filter(df, col("count") >= 1200)
    df = filter(df, col("LateAircraftDelay.mean") >= 10)

    df = groupby(df, "Origin")

    df = agg(df,
             col("count") |> sum |> alias("NumFlights"),
             count() |> alias("NumAirlines"))

    sort(df, "NumAirlines"; rev=true) |> collect
end
