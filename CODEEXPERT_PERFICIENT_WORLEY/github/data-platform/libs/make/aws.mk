.PHONY: aws/export-credentials/worley-data-eng-dev
aws/export-credentials/worley-data-eng-dev:
	@aws configure export-credentials --profile worley-data-eng-dev --format env \
		| sed -z \
		-e 's/export AWS_ACCESS_KEY_ID=/aws_access_key_id="/g' \
		-e 's/export AWS_SECRET_ACCESS_KEY=/aws_secret_access_key="/g' \
		-e 's/export AWS_SESSION_TOKEN=/aws_session_token="/g' \
		-e 's/\n/"\n/g'

.PHONY: aws/export-credentials/worley-data-eng-qa
aws/export-credentials/worley-data-eng-qa:
	@aws configure export-credentials --profile worley-data-eng-qa --format env \
		| sed -z \
		-e 's/export AWS_ACCESS_KEY_ID=/aws_access_key_id="/g' \
		-e 's/export AWS_SECRET_ACCESS_KEY=/aws_secret_access_key="/g' \
		-e 's/export AWS_SESSION_TOKEN=/aws_session_token="/g' \
		-e 's/\n/"\n/g'
