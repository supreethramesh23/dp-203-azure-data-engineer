# Lab 04: Use a SQL Warehouse in Azure Databricks

## Lab Scenario

SQL is an industry-standard language for querying and manipulating data. Many data analysts perform data analytics by using SQL to query tables in a relational database. Azure Databricks includes SQL functionality that builds on Spark and Delta Lake technologies to provide a relational database layer over files in a data lake.

In this lab, you'll learn about Azure Databricks that provides SQL Warehouses that enable data analysts to work with data using familiar relational SQL queries.

### Objectives

After completing this lab, you will be able to:

 - Provision an Azure Databricks workspace
 - View and start a SQL Warehouse
 - Create a database schema
 - Create a table
 - Create a query
 - Create a dashboard

### Estimated timing: 45 minutes

### Architecture Diagram

   ![Azure portal with a cloud shell pane](./Lab-Scenario-Preview/media/lab04-databricks.png)

## Task 1: Provision an Azure Databricks workspace

In this exercise, you'll use a script to provision a new Azure Databricks workspace.

1. In a web browser, sign into the [Azure portal](https://portal.azure.com) at `https://portal.azure.com`.
2. Use the **[\>_]** button to the right of the search bar at the top of the page to create a new Cloud Shell in the Azure portal.

    ![Azure portal with a cloud shell pane](./images/25-1.png)

    >**Note:** If you are not able to see the **[\>_]** button, click on the **ellipses (1)** to the right of the search bar at the top of the page and then select **Cloud Shell (2)** from the drop down options.

    ![Azure portal with a cloud shell pane-ellipses](./images/cloudshell-ellipses.png)

3. Selecting a ***PowerShell*** environment and creating storage if prompted. The cloud shell provides a command line interface in a pane at the bottom of the Azure portal, as shown here:

    ![Azure portal with a cloud shell pane](./images/21051.png)


1. Within the Getting Started pane, select **Mount storage account**, select your **Storage account subscription** from the dropdown and click **Apply**.

   ![](./images/21052.png)

1. Within the **Mount storage account** pane, select **I want to create a storage account** and click **Next**.

   ![](./images/21053.png)


1. If you are prompted to create storage for your Cloud Shell, create storage account as shown in the below screenshot

    ![Create storage by clicking confirm.](./images/21054.png "Create storage advanced settings")

1. Wait for PowerShell terminal to start.


7. In the PowerShell pane, enter the following commands to clone this repo:

    ```
    rm -r dp-203 -f
    git clone -b prod https://github.com/CloudLabs-MOC/dp-203-azure-data-engineer dp-203
    ```

8. After the repo has been cloned, enter the following commands to change to the folder for this lab and run the **setup.ps1** script it contains:

    ```
    cd dp-203/Allfiles/labs/26
    ./setup.ps1
    ```

9. If prompted, choose which subscription you want to use (this will only happen if you have access to multiple Azure subscriptions).

10. Wait for the script to complete - this typically takes around 5 minutes, but in some cases may take longer. While you are waiting, review the [What is data warehousing on Azure Databricks?](https://learn.microsoft.com/azure/databricks/sql/) article in the Azure Databricks documentation.

## Task 2: View and start a SQL Warehouse

1. When the Azure Databricks workspace resource has been deployed, go to it in the Azure portal.
1. In the Azure portal, browse to the **dp203-*xxxxxxx*** resource group that was created by the script (or the resource group containing your existing Azure Databricks workspace)
1. Select your Azure Databricks Service resource (named **databricks*xxxxxxx*** if you used the setup script to create it).

    ![Create storage by clicking confirm.](./images/21055.png)

1. In the **Overview** page for your workspace, use the **Launch Workspace** button to open your Azure Databricks workspace in a new browser tab; signing in if prompted.

    ![Create storage by clicking confirm.](./images/21056.png)

    > **Tip**: As you use the Databricks Workspace portal, various tips and notifications may be displayed. Dismiss these and follow the instructions provided to complete the tasks in this exercise.

1. View the Azure Databricks workspace portal and note that the sidebar on the left side contains links for the various types of task you can perform.
1. In the sidebar, under **SQL**, select **SQL Warehouses**.

![](./images/dbsql1.png)

1. Observe that the workspace already includes a SQL Warehouse named **Starter Warehouse**.
1. In the **Actions** (**&#8285;**) menu for the SQL Warehouse, select **Edit**. Then set the **Cluster size** property to **2X-Small** and save your changes.
1. Use the **Start** button to start the SQL Warehouse (which may take a minute or two).

> **Note**: If your SQL Warehouse fails to start, your subscription may have insufficient quota in the region where your Azure Databricks workspace is provisioned. See [Required Azure vCPU quota](https://docs.microsoft.com/azure/databricks/sql/admin/sql-endpoints#required-azure-vcpu-quota) for details. If this happens, you can try requesting for a quota increase as detailed in the error message when the warehouse fails to start. Alternatively, you can try deleting your workspace and creating a new one in a different region. You can specify a region as a parameter for the setup script like this: `./setup.ps1 eastus`

## Task 3: Create a database schema

1. When your SQL Warehouse is *running*, select **SQL Editor** in the sidebar.
2. In the **Schema browser** pane, observe that the *hive_metastore* catalogue contains a database named **default**.
3. In the **New query** pane, enter the following SQL code:

    ```sql
    CREATE SCHEMA adventureworks;
    ```
4. Use the **&#9658;Run (1000)** button to run the SQL code.
5. When the code has been successfully executed, in the **Schema browser** pane, use the refresh button at the bottom of the pane to refresh the list. Then expand **hive_metastore** and **adventureworks**, and observe that the database has been created, but contains no tables.

You can use the **default** database for your tables, but when building an analytical data store its best to create custom databases for specific data.

## Task 4: Create a table

1. Download the [**products.csv**](https://raw.githubusercontent.com/MicrosoftLearning/dp-203-azure-data-engineer/master/Allfiles/labs/26/data/products.csv) file to your local computer to download the file press ctrl+s, saving it as **products.csv**, .

2. In the Azure Databricks workspace portal, in the sidebar, select **(+) New** and then select **Data** and upload the **products.csv** file you downloaded to your computer.

![](./images/data1.png)

![](./images/data2.png)

3. In the **Add data** page, select the **adventureworks** database from schema dropdown and set the table name to **products**. Then select **Create table** on the bottom right corner of the page.

4. When the table has been created, review its details.

The ability to create a table by importing data from a file makes it easy to populate a database. You can also use Spark SQL to create tables using code. The tables themselves are metadata definitions in the hive metastore, and the data they contain is stored in Delta format in Databricks File System (DBFS) storage.

## Task 5: Create a query

1. In the sidebar, select **(+) New** and then select **Query**.
2. In the **Schema browser** pane, expand **hive_metastore** and **adventureworks**, and verify that the **products** table is listed.
3. In the **New query** pane, enter the following SQL code:

    ```sql
    SELECT ProductID, ProductName, Category
    FROM adventureworks.products; 
    ```

4. Use the **&#9658;Run (1000)** button to run the SQL code.
5. When the query has completed, review the table of results.
6. Use the **Save** button at the top right of the query editor to save the query as **Products and Categories**.

Saving a query makes it easy to retrieve the same data again at a later time.

## Task 6: Create a dashboard

1. In the sidebar, select **(+) New** link and then select **Dashboard**.
2. In the **New dashboard** dialog box, enter the name **Adventure Works Products** and select **Save**.
3. In the **Adventure Works Products** dashboard, in the **Add** drop-down list, select **Visualization**.
4. In the **Add visualization widget** dialog box, select the **Products and Categories** query. Then select **Create new visualization**, set the title to **Products Per Category**, and select **Create visualization**.
5. In the visualization editor, set the following properties:
    - **Visualization type**: bar
    - **Horizontal chart**: selected
    - **Y column**: Category
    - **X columns**: Product ID : Count
    - **Group by**: *Leave blank*
    - **Stacking**: Disabled
    - **Normalize values to percentage**: <u>Un</u>selected
    - **Missing and NULL values**: Do not display in chart

6. Save the visualization and view it in the dashboard.
7. Select **Done editing** to view the dashboard as users will see it.

Dashboards are a great way to share data tables and visualizations with business users. You can schedule the dashboards to be refreshed periodically, and emailed to subscribers.

  **Congratulations** on completing the lab! Now, it's time to validate it. Here are the steps:

  > - Navigate to the Lab Validation tab, from the upper right corner in the lab guide section.
  > - Hit the Validate button for the corresponding task. If you receive a success message, you have successfully validated the lab. 
  > - If not, carefully read the error message and retry the step, following the instructions in the lab guide.
  > - If you need any assistance, please contact us at labs-support@spektrasystems.com.

 ## Review

 In this lab, you have accomplished the following:
  - Provision an Azure Databricks workspace.
 - View and start a SQL Warehouse.
 - Create a database.
 - Create a table.
 - Create a query
 - Create a dashboard
 
 ## You have successfully completed the lab.
