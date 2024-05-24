# Sample Tracking

## Project Overview
Sample Tracking is a project aimed at managing and updating the SQL views of our warehouse. This repository contains SQL scripts and automation workflows to ensure that the warehouse views are kept up-to-date and accurate.

## Setup
The `apply.py` script can be used to update the databases. Run it with `apply.py -h` to see usage notes. You may need to install the Python library `mysql.connector`.
To save you having to input database passwords when you run the script, you can put your passwords into a config file `passwords.ini` with the following format:

```
[uat]
password=uat_password

[prod]
password=prod_password
```
(but don't add the passwords file to the git repository)

## Updating a view
- Create or edit the appropriate file in the `views` folder. Where you have to refer to a database schema, use the following placeholders:
  * `[reporting]`—the reporting schema
  * `[warehouse]`—the warehouse schema
  * `[events]`—the events schema

The names that these refer to can be seen inside the `env.ini` config file.

## Updating UAT Warehouse:
1. Pull Request Approval: Once the pull request is approved, the user needs to proceed with the update process.
2. Execute the `apply.py` script: `./apply.py --uat views/new_view.sql`


## Updating Production Warehouse
1. Testing and Approval: After the changes on the UAT warehouse have been thoroughly tested and approved, the user can proceed with updating the production warehouse.
2. Execute the `apply.py` script: `./apply.py --prod views/new_view.sql`
3. The script will ask you to confirm that you will update production.

## Useful Links
- [Sample tracking report](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Sample+Tracking+Report)
- [Tableau reporting](https://globalreporting.internal.sanger.ac.uk/views/SeqOpsSampleTracking/SampleTracking?:iid=1&:isGuestRedirectFromVizportal=y&:embed=y)

