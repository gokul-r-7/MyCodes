# Glue Job Local Development
You can develop AWS Glue Jobs locally by using AWS Glue Docker images hosted on Docker Hub or the Amazon Elastic Container Registry (Amazon ECR) Public Gallery. The Docker images help you set up your development environment with additional utilities. You can use your preferred IDE, notebook, or REPL using the AWS Glue ETL library.


## Instructions
To run Glue Jobs locally, please follow these steps

1. Pull the image from the Registry
    ```bash
    docker pull amazon/aws-glue-libs:glue_libs_4.0.0_image_01
    ```
2. Add the required environment variables. These will passed on to the Docker container

    ```bash
    export AWS_ACCESS_KEY_ID="YOU_AWS_ACCESS_KEY_ID"
    export AWS_SECRET_ACCESS_KEY="AWS_SECRET_ACCESS_KEY"
    export AWS_SESSION_TOKEN="AWS_SESSION_TOKEN"
    export JUPYTER_WORKSPACE_LOCATION="/YOU/FULL/PATH/TO/LOCAL/FILES"
    ```

3. Run the Docker image
    ```
    docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY -e AWS_SESSION_TOKEN=$AWS_SESSION_TOKEN -e DISABLE_SSL=true -e DATALAKE_FORMATS=iceberg -e --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_4.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
    ```
## Limitations & Documentation
* [AWS Guide](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-using-studio)
* [Local Development Limitations](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#local-dev-restrictions)
