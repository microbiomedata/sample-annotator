-- SQLite
-- before this i created a new table for the mapping called new_env_mapping and
-- dumpped the harmonized table into envo.db 
CREATE TABLE merged AS 
    SELECT * 
    FROM 
        harmonized_wide_sel_envs big_table
    LEFT JOIN new_env_mapping AS map 
        ON 
        big_table.env_broad_scale = map.label;