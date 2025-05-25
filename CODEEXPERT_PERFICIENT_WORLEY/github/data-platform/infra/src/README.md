# Worley Data Engineering Repo
This repository holds the details for Worley's data engineering pipelines and the Worley Data Engineering helper package.

## Documentation
Documentation for this repo is served using `mkdocs`. Please run the following commands to view it.

### Prerequisites
* [Python 3.10+](https://www.python.org/)
* [Pip](https://pip.pypa.io/en/stable/installation/) installed
* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

### Step by step process
1. (Optional) Create a virtual environment in your computer
    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```
2. Install the python libraries
    ```bash
    pip install -r requirements-docs.txt
    ```
3. Run the documentation server
    ```bash
    mkdocs serve
    ```
4. Navigate to [http://127.0.0.1:8000/](http://127.0.0.1:8000/)

