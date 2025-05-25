
#!/bin/bash
#TODO: logic to pass DOMAIN & DASHBOARD ID as input parameters

# Input Arguments
# $1: target environment (QA or PRD)
# $2: Dashboard Domain
# $3: Dashboard ID
# $4: Owner Email Address

#convert $1 to uppercase
TARGET_ENV=$(echo "$1" | tr '[:lower:]' '[:upper:]')

if [ "$TARGET_ENV" = "QA" ]; then
    echo "Target environment is QA"
    . ./qa_config.sh
    echo "QA Data Platform Account ID : $TARGET_DATA_PLATFORM_ACCOUNT"

elif [ "$TARGET_ENV" = "PRD" ]; then
    echo "Target environment is PRD"
    . ./prd_config.sh
    echo "PRD Data Platform Account ID : $TARGET_DATA_PLATFORM_ACCOUNT"
else
    echo " Invalid environment. Please specify either 'QA' or 'PRD' "
    exit 1
fi

echo "Source Data Platform Account ID : $SOURCE_DATA_PLATFORM_ACCOUNT"

#convert domain to lower case
DOMAIN=$(echo "$2" | tr '[:upper:]' '[:lower:]')
DASHBOARD_ID=$3
USERNAME=$4

echo "Updating DataSource ARN within Dataset"
DATASET_DIR="../artifacts/${DOMAIN}/${DASHBOARD_ID}/dataset"
find "$DATASET_DIR" -type f -name "*.json" | while read -r file; do
    if grep -q "dataSourceArn" "$file"; then
        sed -i.bak '/"dataSourceArn":/s|"arn:aws:quicksight:[^"]*"|"'"$DATASOURCE_ARN"'"|g' "$file"
        replacements=$(grep -c "dataSourceArn.*$DATASOURCE_ARN" "$file")
        rm "${file}.bak"
        echo "Updated $replacements dataSourceArn(s) in $file"
    else
        echo "Skipped $file (no dataSourceArn found)"
    fi
done

echo "Finished updating the DataSource ARN in JSON files under '$DATASET_DIR'"

echo "Updating Dataset & Theme ARNs in analysis files"
ANALYSIS_DIR="../artifacts/${DOMAIN}/${DASHBOARD_ID}/analysis"
find "$ANALYSIS_DIR" -type f -name "*.json" | while read -r file; do
    sed -i.bak \
        -e "s/\(arn:aws:quicksight:[^:]*:\)$SOURCE_DATA_PLATFORM_ACCOUNT\(:dataset\)/\1$TARGET_DATA_PLATFORM_ACCOUNT\2/g" \
        -e "s/\(arn:aws:quicksight:[^:]*:\)$SOURCE_DATA_PLATFORM_ACCOUNT\(:theme\)/\1$TARGET_DATA_PLATFORM_ACCOUNT\2/g" \
        "$file"
    rm "${file}.bak"
done
echo "Finished updating ARN's within files in '$ANALYSIS_DIR'"

echo "Updating Dataset, Theme & Analysis ARNs in dashboard files"
DASHBOARD_DIR="../artifacts/${DOMAIN}/${DASHBOARD_ID}/dashboard"
find "$DASHBOARD_DIR" -type f -name "*.json" | while read -r file; do
    sed -i.bak \
        -e "s/\(arn:aws:quicksight:[^:]*:\)$SOURCE_DATA_PLATFORM_ACCOUNT\(:dataset\)/\1$TARGET_DATA_PLATFORM_ACCOUNT\2/g" \
        -e "s/\(arn:aws:quicksight:[^:]*:\)$SOURCE_DATA_PLATFORM_ACCOUNT\(:theme\)/\1$TARGET_DATA_PLATFORM_ACCOUNT\2/g" \
        -e "s/\(arn:aws:quicksight:[^:]*:\)$SOURCE_DATA_PLATFORM_ACCOUNT\(:analysis\)/\1$TARGET_DATA_PLATFORM_ACCOUNT\2/g" \
        "$file"
    rm "${file}.bak"
done
echo "Finished updating ARN's within files in '$DASHBOARD_DIR'"

echo "Creating deployment zip file"
CURRENT_DIR=$(pwd)
cd "../artifacts/${DOMAIN}"

zip -r "${CURRENT_DIR}/${DASHBOARD_ID}-assetbundle-deployment-extract.qs" "./${DASHBOARD_ID}/"

# Verify zip was created successfully
if [ $? -eq 0 ]; then
    echo "Successfully created zip file: $DASHBOARD_ID-assetbundle-deployment-extract.qs"
else
    echo "Error creating zip file"
    exit 100
fi


# #TODO :This can be removed
# echo "Uploading to S3 Bucket"
# aws s3 cp ./"$DASHBOARD_ID"-assetbundle-deployment-extract.qs \
# s3://"$TARGET_S3_BUCKET"/"$DASHBOARD_ID"/ \
# --profile $CURRENT_PROFILE_NAME

cd "../../src"
echo "Starting asset bundle import job for $DASHBOARD_ID"
aws quicksight start-asset-bundle-import-job \
--aws-account-id $TARGET_DATA_PLATFORM_ACCOUNT \
--asset-bundle-import-job-id "$DASHBOARD_ID"-import-job \
--failure-action ROLLBACK \
--asset-bundle-import-source-bytes "fileb://${DASHBOARD_ID}-assetbundle-deployment-extract.qs"
#--asset-bundle-import-source \
#"{\"S3Uri\": \"s3://$TARGET_S3_BUCKET/$DASHBOARD_ID/$DASHBOARD_ID-assetbundle-deployment-extract.qs\"}"

echo "Checking import job status for $DASHBOARD_ID"
while true; do
    JOB_DETAILS=$(aws quicksight describe-asset-bundle-import-job \
        --aws-account-id $TARGET_DATA_PLATFORM_ACCOUNT \
        --asset-bundle-import-job-id $DASHBOARD_ID-import-job \
        --output json)

    JOB_STATUS=$(echo $JOB_DETAILS | jq -r '.JobStatus')
    echo "Current job status: $JOB_STATUS"

    if [ "$JOB_STATUS" = "SUCCESSFUL" ]; then
        echo "Asset bundle import job completed successfully."
        break
    elif [ "$JOB_STATUS" = "IN_PROGRESS" ] || [ "$JOB_STATUS" = "QUEUED_FOR_IMMEDIATE_EXECUTION" ]; then
        echo "Job still in progress. Waiting..."
        sleep 30
    else
        echo "Job failed or in an unexpected state."
        ERRORS=$(echo $JOB_DETAILS | jq -r '.Errors[] | "\(.Type): \(.Message)"')
        if [ -n "$ERRORS" ]; then
            echo "Error details:"
            echo "$ERRORS"
            exit 127
        else
            echo "No specific error details available."
            exit 127
        fi
        break
    fi
done

echo "Updating dashboard permissions for $DASHBOARD_ID"

aws quicksight update-dashboard-permissions \
  --aws-account-id $TARGET_DATA_PLATFORM_ACCOUNT \
  --dashboard-id $DASHBOARD_ID \
  --grant-permissions '[{
    "Principal": "'"arn:aws:quicksight:${AWS_REGION}:${TARGET_DATA_PLATFORM_ACCOUNT}:user/default/${USERNAME}"'",
    "Actions": [
      "quicksight:DescribeDashboard",
      "quicksight:ListDashboardVersions",
      "quicksight:UpdateDashboardPermissions",
      "quicksight:UpdateDashboard",
      "quicksight:DeleteDashboard",
      "quicksight:DescribeDashboardPermissions",
      "quicksight:QueryDashboard",
      "quicksight:UpdateDashboardPublishedVersion"
    ]
  }]'

echo "Deployment process completed for $DASHBOARD_ID"