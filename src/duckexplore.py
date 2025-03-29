import duckdb

def analyze_flights():
    duck_df = duckdb.read_parquet('data/flights.parquet')

    #Filter departure delays greater than 1000 minutes
    duck_df.filter('dep_delay>1000').show()

    # Aggregate average delay per carrier airport
    duck_df.select("carrier,dep_delay").aggregate(" carrier,avg(dep_delay)").show()
    duck_df.select("dest, dep_delay").aggregate("dest, avg(dep_delay)").show()

analyze_flights()

