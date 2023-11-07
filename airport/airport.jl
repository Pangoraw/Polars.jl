using Polars, CSV, Downloads

df = CSV.read("/home/pberg/Downloads/2007.csv", DataFrame; header=true, pool=true, stringtype=String)
df = lazy(df)

airports = CSV.read(Downloads.download("https://raw.githubusercontent.com/plotly/datasets/master/2011_february_us_airport_traffic.csv"), DataFrame; header=true, stringtype=String)

"transforms categorical to int by casting to string"
to_int(c) = col(c) |> cast(String) |> cast(Int)

is_skipped = (col("Diverted") == 1) | (col("Cancelled") == 1)
is_delay_prop = ((col("LateAircraftDelay") > 0) & (col("Delay") > 0)) |> alias("DelayProp")
airlines = let df = df

    df = filter(df, not(is_skipped))

    df = with_columns(
        df,
        fill_null(to_int("ArrDelay") |> alias("Delay"), lit(0)),
        to_int("LateAircraftDelay"),
    )

    df = select(
        df,
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
    df = with_columns(df, col("Origin") |> cast(String))
    df = select(df, "Origin", "NumAirlines", "NumFlights")

    sort(df, "NumAirlines"; rev=true) |> collect
end

df = innerjoin(airlines, airports, "Origin", "iata")
