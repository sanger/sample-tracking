# Sample Tracking

## Project Overview
Sample Tracking is a project aimed at managing and updating the SQL views of our warehouse. This repository contains SQL scripts and automation workflows to ensure that the warehouse views are kept up-to-date and accurate.

## Creating a new patch
- Create an appropriately named SQL file in one of the following folders:
    - `./schema/<file.sql>` for database schema changes
    - `./views/<file.sql>` for alterations to the SQL views
- Add the location of the file to the appropriate place in the `sequence.txt` file, e.g.
    - at the bottom, for patches that will be applied in the next release
    - before `# Production`, for patches that have been applied live to the production database

## Updating UAT Warehouse:
1. Pull Request Approval: Once the pull request is approved, the user needs to proceed with the update process.
2. Script Execution: The user should run the script named execute_sql_uat.sh.
3. Script Functionality: This script generates a runner SQL file, presumably containing the necessary SQL commands to update the UAT warehouse.
4. Password Prompt: During script execution, the user is prompted to enter the password for the UAT warehouse's admin user.

## Updating Production Warehouse
1. Testing and Approval: After the changes on the UAT warehouse have been thoroughly tested and approved, the user can proceed with updating the production warehouse.
2. Script Execution: To update the production warehouse, the user needs to run the script named execute_sql_prod.sh.
3. Script Functionality: Similar to the UAT update script, this script likely generates a runner SQL file with commands to update the production warehouse.
4. Password Prompt: During script execution, the user is prompted to enter the password for the production warehouse's admin user.

## Useful Links
- [Sample tracking report](https://ssg-confluence.internal.sanger.ac.uk/display/PSDPUB/Sample+Tracking+Report)
- [Tableau reporting](https://globalreporting.internal.sanger.ac.uk/views/SeqOpsSampleTracking/SampleTracking?:iid=1&:isGuestRedirectFromVizportal=y&:embed=y)

