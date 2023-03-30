import nmdc_schema.nmdc as nmdc
import linkml
import linkml_runtime.dumpers.json_dumper as dumper
import requests
import json
import logging
from clients.nmdc.runtime_api_client import RuntimeApiSiteClient
from dotenv import dotenv_values
import dateutil.parser as parser



#I had initially misunderstood, and did not design the code as part of an class/object so these are mostly
#independent functions that I've added to this overarching class

#API things are still missing as I only just got access to them ~1 hour before pushing this code up
class SubmissionPortalClient:
    
    #params:
    #mapping_path: file path to mappings between column names
    #env_path: path to env file with API information (not used yet)
    def __init__(self,env_path):
        self.env_path = env_path

        #API things
        self.env_vars = dotenv_values(env_path)

        self.api_client = RuntimeApiSiteClient(self.env_vars['BASE_URL'],self.env_vars['SITE_ID'],self.env_vars['CLIENT_ID'],self.env_vars['CLIENT_SECRET'])

        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(formatter)
        file_handler = logging.FileHandler('submission_portal_log.log')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)
        self.logger = logger
    
    #params:
    #submission_id: a submission_id for the NMDC data portal
    #returns: a json type response containing the data of the NMDC submission
    def get_submission_json(self,submission_id):
        response = requests.request(
            "GET",
            f"https://data.microbiomedata.org/api/metadata_submission/{submission_id}",
            cookies={"session": self.env_vars["DATA_PORTAL_COOKIE"]}
        )
        return response.json()


    def mint_nmdc_ids(self,num_ids):
        json_dic = {"schema_class": {"id": "nmdc:Biosample"},"how_many": num_ids}
        response = self.api_client.request(method="POST",
        url_path="/pids/mint",
        params_or_json_data=json_dic)

        return_lis = json.loads(response.text)
        return return_lis
        
            #Processing the json response from the API
    #Will return a pandas dataframe to be used to make the biosample for the database
    def process_json_response(self,response):
        #There could be more than one type of sampledata in the sampleData lists, so we'll make a dataframe for each one. This also adds some flexibility and robustness

        string_params = ['samp_name', 'analysis_type', 'sample_link', 'ecosystem', 'ecosystem_category', 'ecosystem_type', 'ecosystem_subtype', 'specific_ecosystem', 'fao_class', 'oxy_stat_samp', 'prev_land_use_meth', 'soil_horizon', 'soil_texture_meth', 'tillage', 'water_cont_soil_meth', 'micro_biomass_meth', 'tot_nitro_cont_meth', 'samp_collec_device', 'samp_collec_method', 'rel_to_oxygen', 'collection_time', 'collection_date_inc', 'collection_time_inc', 'start_date_inc', 'start_time_inc', 'filter_method', 'experimental_factor_other', 'non_microb_biomass', 'non_microb_biomass_method', 'microbial_biomass_c', 'micro_biomass_c_meth', 'microbial_biomass_n', 'micro_biomass_n_meth', 'org_nitro_method', 'other_treatment', 'isotope_exposure']
        text_params = ['source_mat_id','env_package', 'agrochem_addition', 'al_sat_meth', 'crop_rotation', 'cur_vegetation', 'cur_vegetation_meth', 'heavy_metals', 'heavy_metals_meth', 'horizon_meth', 'link_class_info', 'link_climate_info', 'local_class', 'misc_param', 'local_class_meth', 'previous_land_use', 'soil_type', 'soil_type_meth', 'ph_meth', 'tot_org_c_meth', 'salinity_meth', 'store_cond', 'geo_loc_name', 'sieving', 'biotic_regm', 'air_temp_regm', 'climate_environment', 'gaseous_environment', 'humidity_regm', 'light_regm', 'watering_regm']
        quantity_params = ['water_content','slope_aspect', 'al_sat', 'annual_precpt', 'annual_temp', 'season_precpt', 'season_temp', 'slope_gradient', 'soil_text_measure', 'temp', 'microbial_biomass', 'carb_nitro_ratio', 'org_matter', 'org_nitro', 'tot_carb', 'tot_nitro_content', 'tot_org_carb', 'tot_phosp', 'phosphate', 'salinity', 'samp_store_temp', 'size_frac_low', 'depth', 'size_frac_up', 'samp_size', 'alt']
        controlled_term_params = ['experimental_factor', 'growth_facil', 'samp_mat_process', 'chem_administration']
        contr_iden_term_params = ['env_broad_scale', 'env_local_scale', 'env_medium']
        float_params = ['elev', 'ph']
        time_params = ['collection_date','extreme_event','fire', 'flooding']
        geoloc_params = ['lat_lon']


        sampledata = response['metadata_submission']['sampleData']
        db_lis = []
        for super_keys in sampledata.keys():
            current_lis = sampledata[super_keys]
            db = nmdc.Database()
            id_counter = 0
            nmdc_ids = self.mint_nmdc_ids(len(current_lis))

            for samp_dic in current_lis:
                param_dic = {}
                for key in samp_dic.keys():

                    if(samp_dic[key] == 'nan' or samp_dic[key] == 'null' or samp_dic[key] == None):
                        continue

                    if (key in string_params):
                        #need to see some examples of how time is put in. May need to change to iso formatting
                        if(";" in samp_dic[key]):
                            temp = samp_dic[key].split(";")
                        else:
                            temp = samp_dic[key]
                        param_dic[key] = temp
                    
                    elif (key in contr_iden_term_params):
                        env_param = samp_dic[key]
                        env_param_lis = env_param.split("[")
                        env_code = env_param_lis[1]
                        env_code = env_code.replace("]","")
                        descriptor = env_param_lis[0].replace("_","")
                        temp = nmdc.ControlledIdentifiedTermValue(has_raw_value=env_param ,term=nmdc.OntologyClass(id=env_code,name=descriptor))
                        param_dic[key] = temp

                    elif (key in controlled_term_params):
                        temp = nmdc.ControlledTermValue(term=nmdc.OntologyClass(id=samp_dic[key]))
                        param_dic[key] = temp

                    elif (key in text_params):
                        #some may need some processing
                        temp = nmdc.TextValue(has_raw_value=samp_dic[key])
                        param_dic[key] = temp

                    elif (key in quantity_params):
                        if(len(samp_dic[key].split()) == 2):
                            quantity_unit = samp_dic[key].split()
                            temp = nmdc.QuantityValue(has_raw_value=samp_dic[key],has_numeric_value = quantity_unit[0] ,has_unit = quantity_unit[1])
                            param_dic[key] = temp
                            
                        elif(key == "depth"):
                            depth_lis = samp_dic[key].split("-")
                            if len(depth_lis) != 2:
                                temp = nmdc.QuantityValue(has_raw_value=samp_dic[key])
                                param_dic[key] = temp
                            else:
                                temp = nmdc.QuantityValue(has_minimum_numeric_value = depth_lis[0],has_maximum_numeric_value = depth_lis[1])
                                param_dic[key] = temp

                        else:
                            temp = nmdc.QuantityValue(has_raw_value=samp_dic[key])
                            param_dic[key] = temp

                    elif (key in float_params):
                        if(key == 'elev'):
                            temp = samp_dic[key].split()
                            elev = float(temp[0])
                            param_dic[key] = elev
                        else:
                            temp = float(samp_dic[key])
                            param_dic[key] = temp

                    elif (key in time_params):
                        iso_date = parser.parse(samp_dic[key]).isoformat()
                        temp = nmdc.TimestampValue(has_raw_value=iso_date)
                        param_dic[key] = temp

                    elif (key in geoloc_params):
                        lat_lon_vals = samp_dic[key].split(',')
                        temp = nmdc.GeolocationValue(has_raw_value=samp_dic[key],latitude=lat_lon_vals[0],longitude=lat_lon_vals[1])
                        param_dic[key] = temp
                    
                    #if something shows up in the else statement it is missing from the mappings
                    else:
                        self.logger.info(key + " not found in any current param list")

                if ('part_of'  not in param_dic.keys()):
                    param_dic['part_of'] = '1000 soils'

                if ('id' not in param_dic.keys()):
                    param_dic['id'] = nmdc_ids[id_counter]
                    id_counter = id_counter + 1

                sample = nmdc.Biosample(**param_dic)

                #For some rason water_content specifically was not being saved in the sample properly when unpacking the dictionary
                if 'water_content' in param_dic.keys():
                    sample['water_content'] = param_dic['water_content']
                db.biosample_set.append(sample)
            db_lis.append(db)
        
            
        return db_lis
            
    
    #dumps the given database to the given filepath
    #params:
    #database: an NMDC database object
    #outfile_path: a string, filepath for desired output
    def dump_db(self,database,outfile_path):
        dumper.dump(element = database, to_file = outfile_path)

    
    