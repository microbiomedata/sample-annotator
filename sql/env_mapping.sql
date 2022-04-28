-- SQLITE
CREATE TABLE new_env_mapping AS
SELECT distinct
	subject as term_id,
	value as label
from
	statements s
where
	predicate = 'rdfs:label'
	and subject like 'ENVO:%';