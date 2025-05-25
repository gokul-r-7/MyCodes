#!/usr/bin/env bash

# Performs AWS SSO login with the given AWS SSO start URL.
# The script uses the AWS CLI to execute the 'aws sso login' command and sets the SSO start URL for the session.
# The temporary credentials obtained from SSO login is then exported to the '~/.aws/credentials' file, which can be utilized by other AWS CLI-enabled applications.

# Usage: aws-sso-login [options]

SSO_SESSION="${1}"
[[ -z "${SSO_SESSION}" ]] && SSO_SESSION="worley-aft-management"

echo "----------------------------------------"
echo "AWS SSO LOGIN - SESSION ${SSO_SESSION}"
echo "----------------------------------------"
aws sso login --sso-session ${SSO_SESSION}
