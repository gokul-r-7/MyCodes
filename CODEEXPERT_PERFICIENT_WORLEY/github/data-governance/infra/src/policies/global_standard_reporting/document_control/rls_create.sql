CREATE RLS POLICY ${domain}_rls_policy
WITH (project_id VARCHAR(100)) 
USING (project_id in (SELECT project_id from ${domain}.${domain}_security where lower(user_name) = lower(current_user)));

GRANT SELECT ON TABLE ${domain}.${domain}_security to RLS POLICY ${domain}_rls_policy;