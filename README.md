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

The `apply.py` script can be used to update the databases. 

You can run it with `apply.py -h` to see usage / help notes. 

- Create or edit the appropriate file in the `views` folder. Where you have to refer to a database schema, use the following placeholders:
  * `[reporting]`—the reporting schema
  * `[warehouse]`—the warehouse schema
  * `[events]`—the events schema

The names that these refer to can be seen inside the `env.ini` config file.

## Updating UAT Warehouse:
1. Pull Request Approval: Once the pull request is approved, the user needs to proceed with the update process.
2. Make sure your are within the virtualenv. 
3. Execute the `apply.py` script: `./apply.py --uat views/<name-of-view>.sql`

## Updating Production Warehouse
1. Testing and Approval: After the changes on the UAT warehouse have been thoroughly tested and approved, the user can proceed with updating the production warehouse.
2. Make sure your are within the virtualenv. 
3. Execute the `apply.py` script: `./apply.py --prod views/<name-of-view>.sql`
4. The script will ask you to confirm that you will update production.

## Tableau

- Tableau runs a daily update on the 2am Long Job schedule, which updates the views. Therefore you may have to wait until the next day to see the changes of running `./apply`.
- To save (if not the original owner), use 'Save As' and enter the name of the report to overwrite.
- You can use the 'Ask Data' to query the data in the report.
- To edit the Filter or Search input in a Dashboard, update the 'Search' function in the left column of the View.
- To Download the full data from Tableau, go to the Dashboard, **click on any header** within the 'View' you would like to download the data for. Click 'Download' > 'Data' > 'Full Data' > 'Download all rows as a text file'. 

## Tableau - Data Source permission error
Laura F was having permission issues viewing the UAT Dashboard.

Matt F said "Its because the UAT data source has been published as its own object with its own permissions. Its been put in the DNA Pipeline project so only those in the DNA pipeline group can access it. The workbook is in the public area so that means someone can open the workbook, but then get denied access to the data. I’ve added her to the access list for the data source so it now should work."

Looking into this more 
- UAT Sample Tracking Data Source entity is in "DNA Pipelines" folder
- Sample Tracking Report UAT Dashboard in is the "Public" folder.
- Sample Tracking Report Prod is in the "Public" folder, but Prod does not have its own Data Source entity

Some questions/ thoughts:
- Should UAT and Production be the same? We could remove the extra UAT Data Source entity, and have UAT connect directly to the DB like Production does?
- Or we move the UAT Data Source entity to the Public folder, to where the View/Dashboards are?
- We do nothing, but for every new user testing in UAT, they have to be added by Matt F to the DNA pipeline group

## Useful Links
- [Sample Tracking - Confluence ](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Sample+Tracking+Report)
- [Sample Tracking Tableau reports](https://globalreporting.internal.sanger.ac.uk/#/search/views?search=sample%20tracking)
- [Sequencing Billing - Confluence](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Automating+Billing+Report) 
- [Sequencing Billing Tableau reports](https://globalreporting.internal.sanger.ac.uk/#/search/views?search=sequencing%20billing)

## Trouble Shooting

Occasionally on `mlwhd` (MLWH UAT), there is an issue in the Sample Tracking view, because the view tries to compare two fields with different collations.

There is error saying `Illegal mix of collations (utf8_unicode_ci,IMPLICIT) and (utf8_general_ci,IMPLICIT) for operation '='`.  This is because BIN_TO_UUID creates a string using the connection's charset/collation, so they have to be set appropriately for the tables they are joining onto. 

Something in `mlwhd` causes this error intermittently. We haven't established why. 

A workaround is to manually set the charset/collation, and recreate the views. 

On your MySQL client, connect to `mlwhd` and run the following:

```
 SET character_set_connection = 'utf8';
 SET collation_connection = 'utf8_unicode_ci';
```

Then re-recreate the views, either using the script (run `./apply`) or by the running the SQL from the file directly into your MySQL client
