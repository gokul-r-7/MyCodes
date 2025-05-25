# Metadata
This repository stores the configuration files for ingesting data from different applications.

# Structure
Each new application should have it's own folder. Inside each folder, you will have your `yaml` files for both `application` level metadata and `table` level metadata.

# How it works
Upon a new commit to the main branch, an event will be triggered to run the `CodePipeline` process that inserts the configuration files into DynamoDB in the respective environment.
