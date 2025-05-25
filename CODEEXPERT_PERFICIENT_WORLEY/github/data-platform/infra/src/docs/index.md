# Worley Data Pipelines
A repository that holds Worley's data pipelines and Worley's python helper package

## Tenets

**Simple**: A simple to use framework that puts the end user (data engineers & business owners) front and centre. The framework aims to remove the need for heavy DevOps/infrastructure and data engineering skills.

**Open**: The framework supports open-source code, open standards, and open frameworks. It also works with popular integrated development environments (IDEs), libraries, and programming languages.

**Extensible**: The framework is designed to be constantly developed and iterated. It takes a modular approach to enable data engineers to reuse components on new use cases


## Architecture

<figure markdown="span">
  ![Reference architecture](./images/architecture.png)
  <figcaption>Reference Architecture</figcaption>
</figure>

## Folder Structure
```
root
│   README.md
│   Makefile
│   requirements-docs.txt
│   mkdocs.yml
│
└───data_pipelines             - Worley's Data Pipeline definitions
│   │
│   │──0_sourcing              - Holds all data pipelines that moves data from `source` to the `raw` layer. Each subfolder refers to one application
│   │  └──aplication_1
│   │  └──aplication_2
│   │
│   └──1_curation              - Holds all data pipelines that moves data from the `raw` layer to the `curated` layer. Each subfolder refers to one table
│      └──table_1
│      └──table_2
│
└───worley_helper              - Worley's Python helper package
│
└───docs                       - Documentation folder based on `mkdocs`

```
