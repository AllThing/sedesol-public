Sedesol
=======================

**Project name**: Enhancing the Distribution of Social Services in Mexico

**Partner**: SEDESOL 

**Issues**: https://github.com/dssg/sedesol/issues

## About 

The Ministry for Social Development (SEDESOL) operates a range of social
service programs to fight poverty in Mexico. One of their major challenges is
how to effectively identify and target the families in most need. DSSG is
working with SEDESOL to develop data-driven methods that can identify: (i) the
socio-economic needs (food, health, education, quality of dwellings, basic
housing services, and social security) of eligible families in poverty, and (ii)
people who might be under-reporting their socio-economic conditions in order to
qualify for programs. We hope that these targeting and detection strategies can
improve the resource allocation and the targeting of programs towards those in
most need. Read more about DSSG and our project
[here](https://dssg.uchicago.edu/).

 
## Technical Plan

Our Technical Plan, which contains the detailed explanation about the project
as well as results and future work, can be found on the `docs/technical-plan`
folder.

## Installation

This project uses [`pyenv`](https://github.com/yyuu/pyenv). We prefer
`pyenv` to `virtualenv` since it is simpler without sacrificing any useful
feature.

Following the standard practice in `pyenv` the version of `python` is specified
in  `.python-version`. In this particular case is `3.5.2`.

### Dependencies

* Python 3.5.2
* luigi
* git
* psql (PostgreSQL) 9.5.4
* PostGIS 2.1.4
* ...and many Python packages

These can be found on the `requirements.txt` file. 

### Docker

To ease the setup, a Dockerfile is provided which builds an image with all
dependencies included and properly configured. Several Docker images are setup
with Docker compose. For further information see the
[`README`](https://github.com/dssg/sedesol/blob/master/infrastructure/README.md)
in `infrastructure`. 

Please take into account that this process downloads and installs all
dependencies.

For information on how to setup Docker, see the official docs.	

## Data Pipeline

Once you have set up the environment, you can start using the pipeline. There
are two different pipelines which can be run either individually or jointly,
pub-imputation and underreporting. The general process of both pipelines is:

* Process data from raw to clean
* Create indexes on clean data for easy joining
* Create semantic tables
* Subset rows
* Load cross-validation indexes
* Get features and responses
* Load train and test data 
* Fit models
* Write model config and evaulation results to database

### Running the pipeline

Run the following

- `git clone https://github.com/dssg/sedesol.git`
- `cd sedesol`
- `make prepare`
- `make deploy`

### External requirements

The pipeline assumes the existence of all raw data already in Postgres tables,
inside a `raw` schema. Instructions and scripts for transforming and uploading
both the data SEDESOL provided and the publicly available data can be found at
the `/etl` folder, specifically on the `db_ingestion*` files.

## TODO

- See the list of [issues](https://github.com/dssg/sedesol/issues)

### Wishful

- Move the data files to `aws s3` and use `smart_open` for a transparent
  write/read operations

