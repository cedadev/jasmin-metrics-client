# Jasmin Metrics Client
test

A library to do requests to elastic search metric data

## Getting started

This Python library was created by CEDA's Backstage service, to begin working on your code you complete 
the steps noted in this document. If you find any issues, please note them in this repo:

https://github.com/cedadev/ceda-github-python-library

### Install Poetry

This library will use the Poetry packaging system. To install Poetry you should follow these 
instructions: https://python-poetry.org/docs/#installation

### Install all Development Libraries

Run `poetry install` to collect all relevant development libraries and create your `poetry.lock` file. 
Alternatively you can run `poetry lock` which will a `poetry.lock`.

In both cases this lock file should be commited.

### Install the pre-commit hooks

Run `poetry run pre-commit install` to install the pre-commit hooks into this repository.

### Activate GitHub workflows

You should change the name of the directory `.rename_github` to `.github` (note the full stops). 
This will activate all GitHub quality assurance, documentation and module publication workflows. 

At this point you can commit all your changes to ensure a fully working and quality assured repository.

### Activate Code Scanning on GitHub

You should activate `Default` "CodeQL analysis" on GitHub at the following link:

https://github.com/cedadev/jasmin-metrics-client/settings/security_analysis

### Create a PyPI project

Create a project on PyPI for jasmin-metrics-client, you should also set up an integration with GitHub 
if you have not done so already.

## Other Configurations

### Check your GitHub email settings (Optional)

If your GitHub account does not make one of your email addresses visible, then commits to this
(and future) libraries made with Backstage templates will not be linked to your GitHub account. 
Backstage will automatically collect this information periodically.
