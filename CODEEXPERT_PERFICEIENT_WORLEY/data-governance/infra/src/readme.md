
## Features

- AWS Lake Formation permission management
- IAM role and policy automation
- Database access control helpers
- Documentation generation tools

## Prerequisites

- Python 3.8+
- AWS CLI configured with appropriate permissions
- Required AWS permissions:
  - LakeFormation permissions
  - IAM role management
  - Glue catalog access

## Installation

1. Clone the repository
    ```bash
    git clone <repository-url>
    cd data-governance
    ```

2. Install dependencies
    ```bash
    pip install -r requirements.txt
    ```

3. Configure AWS credentials
    ```bash
    aws configure
    ```

## Usage

### Lake Formation Helpers

```python
from src.helpers.lakeformation import LakeFormationHelper

# Initialize the helper
lf_helper = LakeFormationHelper()

# Grant permissions to an IAM role
lf_helper.grant_iam_role_permissions_to_lf(
    database="my_database",
    iam_role="arn:aws:iam::account:role/role-name",
    permissions=["SELECT", "DESCRIBE"]
)
