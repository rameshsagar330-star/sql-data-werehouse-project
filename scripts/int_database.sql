/*
                  ============================
                  Create Database and Schemas
                  ============================
Script Porpose:
    This script create a new database named 'Project_Datawarehouse' after checking if it already exists.
    If the database exixts, it is dropped and recreated. Additionally, the script sets up three schemas
    within the database: 'bronze', 'silver' and 'gold'.

WARNING:
    Running this script will drop the entire 'Project_Datawarehouse' Database if it exists.
    All data in the database will be permanantsly deleted.proceed with coution
    and ensure you have proper backups before running this script.
and ensure you have proper backups before running this script.
*/

USE Master;
GO

--Drop and recreate the 'Project_Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Project_Datawarehouse')
BEGIN
  ALTER DATABASE Project_Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE Project_Datawarehouse;
END;
GO

--Create the 'Project_Datawarehouse' Database
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
