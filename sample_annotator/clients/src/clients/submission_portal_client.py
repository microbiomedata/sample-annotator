import pandas as pd
import nmdc_schema.nmdc as nmdc
import linkml
import numpy as np
import linkml_runtime.dumpers.json_dumper as dumper


#I had initially misunderstood, and did not design the code as part of an class/object so these are mostly
#independent functions that I've added to this overarching class

#API things are still missing as I only just got access to them ~1 hour before pushing this code up
class submission_portal_client:
    
    #params:
    #mapping_path: file path to mappings between column names
    #env_path: path to env file with API information (not used yet)
    def __init__(self,mapping_path,env_path):
        self.mapping_path = mapping_path
        self.env_path = env_path
        
    #creates a dictionary of the mappings between columns based on the given file
    #returns: dictionary where the key is the column name from the NMDC portal and the value is the bioSample column name
    def create_mapping_dict(self):
        f = open(self.mapping_path, "r")
        line = f.readline()

        #key and value lists
        map_dict = {}
        #collecting keys and values from file
        while(line != ""):
            pair = line.split(",")
            map_dict[pair[0]] = pair[1].replace("\n","")
            #replacing new line characters with blanks
            line = f.readline()
        f.close()

        return map_dict
    
    # I know we are intending to use the json response from the API, but I havent' been able to use the API
    # For the mean time I've saved the example data in a json file and read that in. 
    # This could be adjusted for the api response with a little tweaking
    #params:
    #data_path: path to the json formatted data file.
    #returns: pandas dataframe
    def read_data_file(self,data_path):
        f = open(data_path, "r")
        line = f.readline()
        rows = []

        #skip through everything that isn't sampleData, we don't need it here
        while('"sampleData": [' not in line):
            line = f.readline()

        #reads until we hit the } denoting the closing of the sampleData block
        line = f.readline()
        while('}' not in line):
            temp_lis = []

            #collecting each row of data, multiple lines per row
            while(']' not in line):
                if(line.strip() != '['):
                    #removing white space, trailing commas, quotation marks
                    line = line.strip().strip('"').strip(",").strip('"')
                    if (line == "depth, meters"):
                        #this comma will mess up the dictionary creation for the map so we remove it here
                        line = "depth meters"
                    elif("GMT" in line):
                        #removing commas again for the dictionary
                        line = line.replace(",","")
                    elif("region" in line):
                        line = line.replace(","," ")

                    temp_lis.append(line)
                line = f.readline()
            line = f.readline()

            #appending the row to the list of rows
            rows.append(temp_lis)

        #The first row isn't useful for our mongoDB ingestion, so we skip over it
        rows.pop()

        #converitng to dataframe
        df = pd.DataFrame(rows[2:len(rows)],columns = rows[1])

        f.close()
        return df
        
    # creates an NMDC database populated with biosamples baed on given mappings and dataframe
    # params:
    # map_dict: dictionary mapping the column names from the NMDC portal and the BioSample columns
    # df: a pandas dataframe created form NMDC portal data
    # returns: an NMDC database object
    def create_biosample_set(self, map_dict, df):
        db = nmdc.Database()

        #Lists for what each value should be converted to. Strings stay as they are, the rest have a function call associated
        #These are all the columns from the example data. It would probably be better to have these in a file and read that in (maybe add a step to the mapping read in to do that at the same time)
        #That is an issue to fix if there is time
        string_params = ["sample name","globally unique ID","analysis/data type","sample linkage","ecosystem","ecosystem_category",
                        "ecosystem_type","ecosystem_subtype","specific_ecosystem","history/extreme events","soil_taxonomic/FAO classification",
                        "history/fire","history/flooding","oxygenation status of sample","history/previous land use method",
                        "soil horizon","soil texture method","history/tillage","water content","water content method","microbial biomass method",
                        "total nitrogen content method","sample collection device","sample collection method","observed biotic relationshiop",
                        "relationship to oxygen","collection time GMT","incubation collection date","incubation collection time GMT",
                        "incubation start date","incubation start time GMT","filter method","experimental factor- other","non-microbial biomass",
                        "non-microbial biomass method","microbial biomass carbon","microbial biomass carbon method","microbial biomass nitrogen",
                        "microbial biomass nitrogen method","organic nitrogen method","other treatments","isotope exposure/addition"]

        text_params = ['environmental package', 'history/agrochemical additions', 'extreme_unusual_properties/Al saturation method', 
                       'history/crop rotation', 'current vegetation', 'current vegetation method', 'extreme_unusual_properties/heavy metals', 
                       'extreme_unusual_properties/heavy metals method', 'horizon method', 'link to classification information', 
                       'link to climate information', 'soil_taxonomic/local classification', 'miscellaneous parameter', 
                       'soil_taxonomic/local classification method', 'history/previous land use', 'soil type', 'soil type method', 'pH method', 
                       'total organic carbon method', 'salinity method', 'storage conditions', 'geographic location (country and/or sea region)', 
                       'composite design/sieving', 'biotic regimen', 'air temperature regimen', 'climate environment', 'gaseous environment', 
                       'humidity regimen', 'light regimen', 'watering regimen']

        quantity_params = ['slope aspect', 'extreme_unusual_properties/Al saturation', 'mean annual precipitation', 'mean annual temperature', 
                           'mean seasonal precipitation', 'mean seasonal temperature', 'slope gradient', 'soil texture measurement', 
                           'temperature', 'microbial biomass', 'carbon/nitrogen ratio', 'organic matter', 'organic nitrogen', 'total carbon', 
                           'total nitrogen content', 'total organic carbon', 'total phosphorus', 'phosphate', 'salinity', 'sample storage temperature', 
                           'size-fraction lower threshold', 'depth meters', 'size-fraction upper threshold', 'amount or size of sample collected', 'altitude']

        controlled_term_params = ['broad-scale environmental context', 'local environmental context', 'environmental medium', 'experimental factor', 
                                  'growth facility', 'sample material processing', 'chemical administration']

        float_params = ["elevation","pH"]

        time_params = ["collection date"]

        geoloc_params = ['geographic location (latitude and longitude)']

        #iterating through each row/sample
        for index, row in df.iterrows():
            param_dic = {}

            #doing necessary conversions and filtering of each column/slot
            for col in df.columns:
                if (row[col] == 'null' or row[col] == 'nan' or row[col] == None):
                    continue

                if (col in string_params):
                    if (col == 'globally unique ID'):
                        key = map_dict[col]
                        param_dic[key] = '0' #need to mint an nmdc unique ID here via the API
                        param_dic['alternative_identifiers'] = row[col] #this vlue might need to be different than 'alternative_identifiers'

                    else:
                        key = map_dict[col]
                        if(";" in row[col]):
                            temp = row[col].split(";")
                        else:
                            temp = row[col]

                        param_dic[key] = temp

                elif (col in controlled_term_params):
                    #need to add some kind of processing for the environmental context params
                    #look at the ontology class documentation as well for what params are used

                    #convert then add to dictionary
                    temp = nmdc.ControlledTermValue(term=nmdc.OntologyClass(id=row[col]))
                    key = map_dict[col]
                    param_dic[key] = temp

                elif (col in text_params):
                    #some may need some processing
                    temp = nmdc.TextValue(has_raw_value=row[col])
                    key = map_dict[col]
                    param_dic[key] = temp

                elif (col in quantity_params):
                    #some may need some additional processing
                    if(col == "sample storage temperature"):
                        temp_lis = row[col].split()
                        temp = nmdc.QuantityValue(has_raw_value = temp_lis[0],has_unit = temp_lis[1])
                        key = map_dict[col]
                        param_dic[key] = temp

                    elif(col == "depth meters"):
                        depth_lis = row[col].split("-")
                        if len(depth_lis) != 2:
                            temp = nmdc.QuantityValue(has_raw_value=row[col])
                            key = map_dict[col]
                            param_dic[key] = temp
                        else:
                            temp = nmdc.QuantityValue(has_minimum_numeric_value = depth_lis[0],has_maximum_numeric_value = depth_lis[1])
                            key = map_dict[col]
                            param_dic[key] = temp

                    else:
                        temp = nmdc.QuantityValue(has_raw_value=row[col])
                        key = map_dict[col]
                        param_dic[key] = temp

                elif (col in float_params):
                    if(col == 'elevation'):
                        temp = row[col].split()
                        elev = float(temp[0])
                        key = map_dict[col]
                        param_dic[key] = elev
                    else:
                        temp = float(row[col])
                        key = map_dict[col]
                        param_dic[key] = temp

                elif (col in time_params):
                    #may need some processing
                    temp = nmdc.TimestampValue(has_raw_value=row[col])
                    key = map_dict[col]
                    param_dic[key] = temp

                elif (col in geoloc_params):
                    temp = nmdc.GeolocationValue(has_raw_value=row[col])
                    key = map_dict[col]
                    param_dic[key] = temp
                
                #if something shows up in the else statement it is missing from the mappings
                else:
                    print(col + " not found in any current param list")

            if ('part_of'  not in param_dic.keys()):
                param_dic['part_of'] = '1000 soils'

            if ('id' not in param_dic.keys()):
                param_dic['id'] = '0' #mint a key via the api

            #I don't think any of these should be blank in real data, but they are needed here since the example data is blank
            #this block of code should be removed once real data is in
            missing_env = [item for item in ["env_broad_scale","env_local_scale","env_medium"] if item not in param_dic.keys()]
            for item in missing_env:
                temp = nmdc.ControlledTermValue(term=nmdc.OntologyClass(id="0000"))
                param_dic[item] = temp

            sample = nmdc.Biosample(**param_dic)
            db.biosample_set.append(sample)
            
        return db
    
    #dumps the given database to the given filepath
    #params:
    #database: an NMDC database object
    #outfile_path: a string, filepath for desired output
    def dump_db(self,database,outfile_path):
        dumper.dump(element = database, to_file = outfile_path)

    
    