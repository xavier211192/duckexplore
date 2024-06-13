import duckdb

duck_df = duckdb.read_parquet('data/flights.parquet')
# duck_df.limit(10).show()


#Filter departure delays greater than 1000 minutes
duck_df.filter('dep_delay>1000').show()

# Aggregate average delay per destination airport
duck_df.select("carrier,dep_delay").aggregate(" carrier,avg(dep_delay)").show()
