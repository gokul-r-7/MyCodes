# Terraform Enterprise Workflow for Team Development

A well-defined workflow is crucial for efficiently managing infrastructure changes in a team environment. In this documentation, I'll outline the recommended process for developing and deploying new features to your Terraform Enterprise workspace, complete with graphical representations of the workflow.

## Workflow Overview

The following diagram illustrates the overall workflow for team development using Terraform Enterprise:

- Create Feature Branch
- Develop and Test Locally
- Commit and Push to VCS
- Create Pull Request
- Automated Runs in Terraform Enterprise
- Review and Merge
- Continuous Deployment
- Run Speculative Plan in Terraform Cloud
- Back to Develop and Test Locally

Let's dive into the details of each step:

### 1. Create Feature Branch

Each team member should create a new branch from the `main` branch to work on their feature development. This allows parallel development and isolates changes from the main codebase.

### 2. Develop and Test Locally

Team members should work on their feature branches, making changes to the Terraform configuration, and testing the changes locally. This may involve running `terraform plan` on their local machines.

### 3. Run Speculative Plan in Terraform Cloud

Developers can also log in to Terraform Cloud from their local CLI and run speculative plans to preview the changes before committing them to the VCS. This allows for early feedback and validation of the changes.

### 4. Commit and Push to VCS

Once the team member is satisfied with their changes, they should commit the changes to their feature branch in the version control system (VCS) used by your Terraform Enterprise workspace (e.g., GitHub).

### 5. Create Pull Request

The team member should create a pull request (PR) to merge their feature branch into the `main` branch. This allows other team members to review the changes and provide feedback.

**Note:** Every time you create a pull request, a plan-only job will be triggered in Terraform Enterprise (TFE). This allows you to review the proposed changes before applying them.

### 6. Automated Runs in Terraform Enterprise

When the pull request is created or updated, Terraform Enterprise should automatically trigger a new run for the workspace, which will apply the changes from the feature branch to the workspace environment (e.g., the AWS account).

### 7. Run Speculative Plan Locally (Optional)

If you want to run a speculative plan locally without creating a pull request, you can follow these steps:

1. Navigate to the `infra` directory in your project.
2. Create an API Token in Terraform Enterprise by visiting the [API Tokens](https://app.terraform.io/app/settings/tokens?organization_name=W-GlobalDatacenterServices) page.
3. Run `terraform login` and paste the API Token when prompted.
4. Run `terraform plan` to perform a speculative plan on the workspace.

This allows you to preview the changes locally without the need for a pull request.

### 8. Review and Merge

Once the changes have been reviewed and approved, the pull request can be merged into the `main` branch. This will update the main codebase with the new feature.

### 9. Continuous Deployment

Subsequent pushes to the `main` branch should automatically trigger new runs in Terraform Enterprise, applying the changes to the workspace environment.

## Visual Representation of the Workflow

To better illustrate the workflow, let's use the Mermaid syntax to create a diagram:

- Team Development:
  - Feature Branch Development
  - Run Speculative Plan in Terraform Cloud
  - Commit & Push to VCS
  - Pull Request
  - Automated Runs in Terraform Enterprise
  - Review & Merge
- Continuous Deployment:
  - Pushes to `main`
  - Automated Runs in Terraform Enterprise

This diagram showcases the clear separation between the team development process, including the ability to run speculative plans in Terraform Cloud, and the continuous deployment aspect, highlighting the key steps and their connections.

## Conclusion

By following this well-defined workflow, your team can effectively develop new features, test them, and deploy the changes to the Terraform Enterprise workspace environment in a controlled and reliable manner. The combination of branching, automated runs, speculative plans, and continuous deployment ensures that your infrastructure remains up-to-date and aligned with the evolving requirements of your organization.
