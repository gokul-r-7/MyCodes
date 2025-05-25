#!/bin/bash

DOMAIN=$(echo "$1" | tr '[:upper:]' '[:lower:]')
DASHBOARD_ID=$2

echo "Starting asset bundle export job for $DASHBOARD_ID"
. ./dev_config.sh

echo "Dev Data Platform Account ID : $DEV_DATA_PLATFORM_ACCOUNT"

aws quicksight start-asset-bundle-export-job \
--aws-account-id $DEV_DATA_PLATFORM_ACCOUNT \
--asset-bundle-export-job-id $DASHBOARD_ID-export-job \
--include-all-dependencies \
--export-format QUICKSIGHT_JSON \
--resource-arns "arn:aws:quicksight:$AWS_REGION:$DEV_DATA_PLATFORM_ACCOUNT:dashboard/$DASHBOARD_ID"

echo "Checking export job status for $DASHBOARD_ID"
while true; do
    JOB_DETAILS=$(aws quicksight describe-asset-bundle-export-job \
        --aws-account-id $DEV_DATA_PLATFORM_ACCOUNT \
        --asset-bundle-export-job-id  $DASHBOARD_ID-export-job \
        --output json)

    JOB_STATUS=$(echo $JOB_DETAILS | jq -r '.JobStatus')
    echo "Current job status: $JOB_STATUS"

    if [ "$JOB_STATUS" = "SUCCESSFUL" ]; then
        ASSET_DOWNLOAD_URL=$(echo $JOB_DETAILS | jq -r '.DownloadUrl')
        echo "Job completed successfully. Download URL: $ASSET_DOWNLOAD_URL"
        break
    elif [ "$JOB_STATUS" = "FAILED" ]; then
        echo "Job failed with the following errors:"

        # Check if Errors array exists and is not empty
        if [ "$(echo $JOB_DETAILS | jq 'has("Errors") and (.Errors | length > 0)')" = "true" ]; then
            # Traverse through the Errors array
            echo $JOB_DETAILS | jq -r '.Errors[] | "Error Type: \(.Type)\nError Message: \(.Message)\n"'
        else
            echo "No detailed error information available."
        fi
        exit 1
    else
        echo "Job still in progress. Waiting..."
        sleep 30
    fi
done

echo "Downloading and extracting asset bundle"
wget -O "../artifacts/${DOMAIN}/${DASHBOARD_ID}-assetbundle-extract.qs" "$ASSET_DOWNLOAD_URL"


# Remove existing directory if it exists
rm -rf "../artifacts/${DOMAIN}/${DASHBOARD_ID}"

unzip -o "../artifacts/${DOMAIN}/${DASHBOARD_ID}-assetbundle-extract.qs" -d "../artifacts/${DOMAIN}/${DASHBOARD_ID}/"

# Removing VPC Connection & Data Source Files as they are already created
rm -r "../artifacts/${DOMAIN}/${DASHBOARD_ID}/vpcConnection"
rm -r "../artifacts/${DOMAIN}/${DASHBOARD_ID}/datasource"

#removing asset bundle extract file
rm  "../artifacts/${DOMAIN}/${DASHBOARD_ID}-assetbundle-extract.qs"

# add or update timestamp in file name to all files within "../artifacts/${DOMAIN}/${DASHBOARD_ID}""
TIMESTAMP=$(date -u +"%Y%m%d%H%M%S")
find "../artifacts/${DOMAIN}/${DASHBOARD_ID}" -type f -name "*.json" -exec sh -c '
    timestamp=$1
    shift # Remove the timestamp argument
    for file do
        # Extract base name up to the last "-" if it exists and has digits after it
        if echo "$file" | grep -q ".*-[0-9]\{8,\}\.json$"; then
            # Get everything before the last "-"
            base_name=$(echo "$file" | sed "s/-[0-9]\{8,\}\.json$//")
            new_name="${base_name}-${timestamp}.json"
        else
            # No timestamp exists, add new one
            new_name="${file%.json}-${timestamp}.json"
        fi
        mv "$file" "$new_name"
    done
' sh "${TIMESTAMP}" {} +

