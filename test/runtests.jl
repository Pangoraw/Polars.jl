using Polars, Test

@testset "Create from C Data interface" begin
    table = (; x = randn(Float32, 100))

    df = DataFrame(table)
    s = only(select(df, col("x") |> sum)[:x])

    @test s ≈ sum(table.x)

    df = nothing
end

@testset "GC C Data interface" begin
    GC.gc(true)

    @test isempty(Polars.LIVE_ARRAYS)
    @test isempty(Polars.LIVE_SCHEMAS)
end

@testset "Lazy vs Eager" begin
    table = (; x=randn(Float32, 100), cond = rand(Bool, 100))
    df = DataFrame(table)

    function selector(df)
        df = with_columns(df, cos(col("x")*1.5) |> alias("tmp"))
        filter(df, col("cond") & (col("x") < 0.))
    end

    df2 = df |> lazy |> selector |> collect
    df = selector(df)

    @test df[:tmp] == df2[:tmp]
end

@testset "Exprs" begin
    df = DataFrame((; x=[1,2,3,3.1,missing]))

    @test filter(df, col("x") >= 2) |> size == (3,1)
    @test filter(df, col("x") > 2)  |> size == (2,1)
    @test filter(df, col("x") == 2) |> size == (1,1)

    @test filter(df, col("x") |> is_null) |> size == (1,1)
    @test filter(df, col("x") |> is_null |> Polars.not) |> size == (4,1)

    df = DataFrame((; names = ["john", "alice", missing, "bob", "lilly"]))

    lengths = select(df, col("names") |> Strings.lengths_chars |> sum |> suffix("_lengths"))[:names_lengths] |> only
    @test lengths == length("john") + length("alice") + length("bob") + length("lilly")

    df = DataFrame((; names = ["eggs 🥚", "cheese 🧀", "tomatoes 🍅"],
                      price = [1.2, 3.4, 5.4],
                      availability = [20, 2, 3]))
    df = filter(df, (col("price") * col("availability")) < 10.)
    df = select(df, col("names") |> Strings.uppercase |> alias("tobuy"))

    @test df[:tobuy] == ["CHEESE 🧀"]
end

@testset "Series" begin
    values = [1,2,3,4,5]
    s = Series(:values, values)
    @test sum(values) == sum(s)
end
