import sys
from data_fetcher import DataFetcher
from json_gen import JSONGenerator
from history_gen import generate_history_for_strm


# read strm from command line
strm = int(sys.argv[1])

# strm = 1242
database_path = f"data_{strm}.db"
table_name = "sessions"

num_pages_in_batch = 150    # make sure this is more than number of pages in sis

# fetch data, update db
fetcher = DataFetcher(database_path, table_name, strm, num_pages_in_batch)
fetcher.run()

# generate json files
json_gen = JSONGenerator(database_path, table_name, strm)
json_gen.generate()

# update the published enrollment history snapshot for this semester
generate_history_for_strm(strm)
