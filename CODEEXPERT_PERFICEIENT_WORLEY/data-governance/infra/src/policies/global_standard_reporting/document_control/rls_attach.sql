ATTACH RLS POLICY ${domain}_rls_policy
ON ${domain}.${obt_name}
TO ${grant_statement};

ALTER table ${domain}.${obt_name} ROW LEVEL security on