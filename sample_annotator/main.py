import sys
from clients.submission_portal_client import SubmissionPortalClient



client = SubmissionPortalClient('.env')
#r = client.mint_nmdc_ids(4)
r = client.get_submission_json('353d751f-cff0-4558-9051-25a87ba00d3f')
biosample_db_lis = client.process_json_response(r)
for i in range(0,len(biosample_db_lis)):
    client.dump_db(biosample_db_lis[i],"output_" + str(i) + ".json")