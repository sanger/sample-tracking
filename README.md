# Sample Tracking

## Project Overview
Sample Tracking is a project aimed at managing and updating the SQL views of our warehouse. This repository contains SQL scripts and automation workflows to ensure that the warehouse views are kept up-to-date and accurate.

## Setup

- Install `pipenv`, if you haven't already 

```shell
brew install pipenv
```

- Create a virtualenv for this project (this creates the Pipfile using your Homebrews default Python), and install any dependencies

```shell
pipenv install
```

- To activate this project's virtualenv, run

```shell
pipenv shell
```

- To install any **new** packages, from within a `pipenv shell` run

```shell
pipenv install <package-name>
```

## Credentials

To save you having to input database passwords when you run the script, you can put your passwords into a config file `passwords.ini` with the following format:

```
[uat]
password=uat_password

[prod]
password=prod_password
```

The passwords are in KeePass (but don't add the passwords file to the git repository)

## Apply changes

Make sure your are within the virtualenv. 

The `apply.py` script can be used to update the databases. 

You can run it with `apply.py -h` to see usage / help notes. 

- Create or edit the appropriate file in the `views` folder. Where you have to refer to a database schema, use the following placeholders:
  * `[reporting]`—the reporting schema
  * `[warehouse]`—the warehouse schema
  * `[events]`—the events schema

The names that these refer to can be seen inside the `env.ini` config file.

## Updating UAT Warehouse:
1. Pull Request Approval: Once the pull request is approved, the user needs to proceed with the update process.
2. Execute the `apply.py` script: `./apply.py --uat views/<name-of-view>.sql`


## Updating Production Warehouse
1. Testing and Approval: After the changes on the UAT warehouse have been thoroughly tested and approved, the user can proceed with updating the production warehouse.
2. Execute the `apply.py` script: `./apply.py --prod views/<name-of-view>.sql`
3. The script will ask you to confirm that you will update production.

## Useful Links
- [Sample tracking report](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Sample+Tracking+Report)
- [Tableau reporting](https://globalreporting.internal.sanger.ac.uk/views/SeqOpsSampleTracking/SampleTracking?:iid=1&:isGuestRedirectFromVizportal=y&:embed=y)

## Trouble Shooting
Occasionally on mlwhd, when query the views, there is error saying 'Illegal mix of collations (utf8_unicode_ci,IMPLICIT) and (utf8_general_ci,IMPLICIT) for operation '=''.  This is because BIN_TO_UUID creates a string using the connection's charset/collation, so they have to be set appropriately for the tables they are joining onto. By running the following:

SET character_set_connection = 'utf8';
SET collation_connection = 'utf8_unicode_ci';

then rerun the view creation SQL should solve this problem.

