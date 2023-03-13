import sys
from clients.submission_portal_client import submission_portal_client

client = submission_portal_client('../mappings.csv','no_path')
map_dict = client.create_mapping_dict()
df = client.read_data_file('../example_data.json')
biosample_db = client.create_biosample_set(map_dict,df)
client.dump_db(biosample_db,"output.json")