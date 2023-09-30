from data_fetcher import DataFetcher
from json_gen import JSONGenerator

database_path = "data.db"
table_name = "sessions"
strm = 1238
num_pages_in_batch = 100    # make sure this is more than number of pages in sis

# fetch data, update db
fetcher = DataFetcher(database_path, table_name, strm, num_pages_in_batch)
fetcher.run()

# generate json files
json_gen = JSONGenerator(database_path, table_name, strm)
json_gen.generate()