CREATE RLS POLICY ${domain}_rls_policy
WITH (integration_id VARCHAR(100))
USING (integration_id in (SELECT integration_id from ${domain}.${domain}_security where lower(user_name) = lower(current_user)));

GRANT SELECT ON TABLE ${domain}.${domain}_security to RLS POLICY ${domain}_rls_policy;