import sys
from clients.submission_portal_client import submission_portal_client



client = submission_portal_client('.env')
r = client.get_submission_json('e4cf7c2c-29b4-4924-8452-2bd3c8e518db')
biosample_db_lis = client.process_json_response(r)
for i in range(0,len(biosample_db_lis)):
    client.dump_db(biosample_db_lis[i],"output_" + str(i) + ".json")