# Query files using a serverless SQL pool

SQL is probably the most used language for working with data in the world. Most data analysts are proficient in using SQL queries to retrieve, filter, and aggregate data - most commonly in relational databases. As organizations increasingly take advantage of scalable file storage to create data lakes, SQL is often still the preferred choice for querying the data. Azure Synapse Analytics provides serverless SQL pools that enable you to decouple the SQL query engine from the data storage and run queries against data files in common file formats such as delimited text and Parquet.

This lab will take approximately **40** minutes to complete.


## Query data in files

This Lab provisioned with Azure Synapse Analytics workspace and an Azure Storage account to host the data lake, and also uploaded some data files to the data lake to perform the below tasks.

### Task-1: View files in the data lake

1. In the Azure portal, Click on Show Portal Menu and select **Resource groups**

   ![Screenshot showing the selection of Resource groups ](../images/DP500-1-1.png)
   
1. Inside the Resource groups,select **lab01-rg**
 
   ![Screenshot showing the selection of lab01 Resource groups ](../images/DP500-1-2.png)
   
1. Select the Synapse workspace titled **workspace<inject key="DeploymentID" enableCopy="false"/>** 

   ![Screenshot showing the selection of Synapse workspace ](../images/DP500-1-3.png)
   
1. In the **Overview** page for your Synapse workspace, in the **Open Synapse Studio** card in getting started, select **Open** to open Synapse Studio in a new browser tab and do the  sign in if prompted with the credientials provided in the Environment details tab.

   ![Screenshot showing the selection of open button to navigate to synapse studio ](../images/DP500-1-4.png)
   
1. On the left side of Synapse Studio, use the **&rsaquo;&rsaquo;** icon to expand the menu - this reveals the different pages within Synapse Studio that you'll use to manage resources and perform data analytics tasks.

1. On the **Data** page, view the **Linked** tab and verify that your workspace includes a link to your Azure Data Lake Storage Gen2 storage account, which should have a name similar to **workspace<inject key="DeploymentID" enableCopy="false"/>** (Primary - **datalake<inject key="DeploymentID" enableCopy="false"/>**)**.

   ![Screenshot showing the Linked storage account with synapse ](../images/DP500-1-5.png) 
   
1. Expand your storage account and verify that it contains a file system container named **files** inside **workspace<inject key="DeploymentID" enableCopy="false"/>**

1. Select the **files** container, and note that it contains a folder named **sales**. This folder contains the data files you are going to query.

   ![Screenshot showing the Linked storage account with synapse ](../images/DP500-1-6.png)
   
1. Open the **sales** folder,you can see the three sub folders titled **csv**.**json** and **parquet** folders.

   ![Screenshot showing the subfolders in the sales folder ](../images/DP500-1-7.png)
   
1. In the **sales** folder, open the **csv** folder, and observe that this folder contains .csv files for three years of sales data.

   ![Screenshot showing the files in the CSV folder ](../images/DP500-1-8.png)
   
1. Right-click any of the files and select **Preview** to see the data it contains. Note that the files do not contain a header row, so you can unselect the option to display column headers.

   ![Screenshot showing the PREVIEW selection ](../images/DP500-1-9.png)
   
   ![Screenshot showing the closing of PREVIEW selection ](../images/DP500-1-10.png)
   
1. Close the preview by clicking on **OK**, and then use the **&#8593;** button to navigate back to the **sales** folder.

1. In the **sales** folder, open the **json** folder and observe that it contains some sample sales orders in .json files. Preview any of these files to see the JSON format used for a sales order.

1. Close the preview, and then use the **&#8593;** button to navigate back to the **sales** folder.

1. In the **sales** folder, open the **parquet** folder and observe that it contains a subfolder for each year (2019-2021), in each of which a file named **orders.snappy.parquet** contains the order data for that year. 

1. Return to the **sales** folder so you can see the **csv**, **json**, and **parquet** folders.

### Task-2: Use SQL to query CSV files

1. Select the **csv** folder, and then in the **New SQL script** list on the toolbar, select **Select TOP 100 rows**.

   ![Screenshot showing the steps ](../images/DP500-1-11.png)
   
1. In the **File type** list, select **Text format**, and then apply the settings to open a new SQL script that queries the data in the folder.

   ![Screenshot showing the steps ](../images/DP500-1-12.png)
   
1. In the **Properties** pane for **SQL Script 1** that is created, change the name to **Sales CSV query**, and change the result settings to show **All rows**. Then in the toolbar, select **Publish** to save the script and use the **Properties** button (which looks similar to **&#128463;.**) on the right end of the toolbar to hide the **Properties** pane.

   ![Screenshot showing the steps](../images/DP500-1-13.png)
   
1. Review the SQL code that has been generated, which should be similar to this:

    ```SQL
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0'
        ) AS [result]
    ```

    This code uses the OPENROWSET to read data from the CSV files in the sales folder and retrieves the first 100 rows of data.

1. In the **Connect to** list, ensure **Built-in** is selected - this represents the built-in SQL Pool that was created with your workspace.

   ![Screenshot showing the steps](../images/DP500-1-14.png)
   
1. On the toolbar, use the **&#9655; Run** button to run the SQL code, and review the results, which should look similar to this:

   ![Screenshot showing the steps](../images/DP500-1-15.png)

1. Note the results consist of columns named C1, C2, and so on. In this example, the CSV files do not include the column headers. While it's possible to work with the data using the generic column names that have been assigned, or by ordinal position, it will be easier to understand the data if you define a tabular schema. To accomplish this, add a WITH clause to the OPENROWSET function as shown here (replacing *datalakexxxxxxx* with the name of your data lake storage account), and then rerun the query:

    ```SQL
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/csv/',
            FORMAT = 'CSV',
            PARSER_VERSION='2.0'
        )
        WITH (
            SalesOrderNumber VARCHAR(10) COLLATE Latin1_General_100_BIN2_UTF8,
            SalesOrderLineNumber INT,
            OrderDate DATE,
            CustomerName VARCHAR(25) COLLATE Latin1_General_100_BIN2_UTF8,
            EmailAddress VARCHAR(50) COLLATE Latin1_General_100_BIN2_UTF8,
            Item VARCHAR(30) COLLATE Latin1_General_100_BIN2_UTF8,
            Quantity INT,
            UnitPrice DECIMAL(18,2),
            TaxAmount DECIMAL (18,2)
        ) AS [result]
    ```
    
    ![Screenshot showing the steps](../images/DP500-1-16.png)

    Now the results look like this:
  
    ![Screenshot showing the steps](../images/DP500-1-17.png)

8. Publish the changes to your script, and then close the script pane.
    
    ![Screenshot showing the steps](../images/DP500-1-18.png)

### Task-3: Use SQL to query parquet files

While CSV is an easy format to use, it's common in big data processing scenarios to use file formats that are optimized for compression, indexing, and partitioning. One of the most common of these formats is *parquet*.

1. In the **files** tab contaning the file system for your data lake, return to the **sales** folder so you can see the **csv**, **json**, and **parquet** folders.

1. Select the **parquet** folder, and then in the **New SQL script** list on the toolbar, select **Select TOP 100 rows**.

   ![Screenshot showing the steps](../images/DP500-1-19.png)
   
1. In the **File type** list, select **Parquet format**, and then apply the settings to open a new SQL script that queries the data in the folder.

   ![Screenshot showing the steps](../images/DP500-1-20.png)
   
1. The script should look similar to this:

    ```SQL
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/**',
            FORMAT = 'PARQUET'
        ) AS [result]
    ```
    ![Screenshot showing the steps](../images/DP500-1-21.png)
    
1. Run the code, and note that it returns sales order data in the same schema as the CSV files you explored earlier. The schema information is embedded in the parquet file, so the appropriate column names are shown in the results.

    ![Screenshot showing the steps](../images/DP500-1-22.png)
    
1. Modify the code as follows (replacing *datalakexxxxxxx* with the name of your data lake storage account as **datalake<inject key="DeploymentID" enableCopy="false"/>** and then run it.

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           COUNT(*) AS OrderedItems
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/**',
            FORMAT = 'PARQUET'
        ) AS [result]
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear
    ```
    ![Screenshot showing the steps](../images/DP500-1-23.png)
    
1. Note that the results include order counts for all three years - the wildcard used in the BULK path causes the query to return data from all subfolders.

    ![Screenshot showing the steps](../images/DP500-1-24.png)

    The subfolders reflect *partitions* in the parquet data, which is a technique often used to optimize performance for systems that can process multiple partitions of data in parallel. You can also use partitions to filter the data.

1. Modify the code as follows (replacing *datalakexxxxxxx* with the name of your data lake storage account as **datalake<inject key="DeploymentID" enableCopy="false"/>** and then run it.

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           COUNT(*) AS OrderedItems
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/parquet/year=*/',
            FORMAT = 'PARQUET'
        ) AS [result]
    WHERE [result].filepath(1) IN ('2019', '2020')
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear
    ```
    ![Screenshot showing the steps](../images/DP500-1-25.png)
    
1. Review the results and note that they include only the sales counts for 2019 and 2020. This filtering is achieved by inclusing a wildcard for the partition folder value in the BULK path (*year=\**) and a WHERE clause based on the *filepath* property of the results returned by OPENROWSET (which in this case has the alias *[result]*).

    ![Screenshot showing the steps](../images/DP500-1-26.png)

1. Name your script **Sales Parquet query** by clicking the Properties tab icon, and publish it. Then close the script pane.

     ![Screenshot showing the steps](../images/DP500-1-27.png)

### Task-4: Use SQL to query JSON files

JSON is another popular data format, so it;s useful to be able to query .json files in a serverless SQL pool.

1. In the **files** tab containing the file system for your data lake, return to the **sales** folder so you can see the **csv**, **json**, and **parquet** folders.

1. Select the **json** folder, and then in the **New SQL script** list on the toolbar, select **Select TOP 100 rows**.

   ![Screenshot showing the steps](../images/DP500-1-28.png)
   
1. In the **File type** list, select **Text format**, and then apply the settings to open a new SQL script that queries the data in the folder.

    ![Screenshot showing the steps](../images/DP500-1-29.png)
    
1.  The script should look similar to this:

    ```sql
    -- This is auto-generated code
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0'
        ) AS [result]
    ```
    
    ![Screenshot showing the steps](../images/DP500-1-30.png)
    
    The script is designed to query comma-delimited (CSV) data rather then JSON, so you need to make a few modifications before it will work successfully.

1. Modify the script as follows (replacing *datalakexxxxxxx* with the name of your data lake storage account as **datalake<inject key="DeploymentID" enableCopy="false"/>** to:
    - Remove the parser version parameter.
    - Add parameters for field terminator, quoted fields, and row terminators with the character code *0x0b*.
    - Format the results as a single field containing the JSON row of data as an NVARCHAR(MAX) string.

    ```sql
    SELECT
        TOP 100 *
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            FIELDTERMINATOR ='0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
        ) WITH (Doc NVARCHAR(MAX)) as rows
    ```
   ![Screenshot showing the steps](../images/DP500-1-31.png)
   
1. Run the modified code and observe that the results include a JSON document for each order.

   ![Screenshot showing the steps](../images/DP500-1-32.png)

1. Modify the query as follows (replacing *datalakexxxxxxx* with the name of your data lake storage account as **datalake<inject key="DeploymentID" enableCopy="false"/>** so that it uses the JSON_VALUE function to extract individual field values from the JSON data.View the result by running the query after modifying as below.

    ```sql
    SELECT JSON_VALUE(Doc, '$.SalesOrderNumber') AS OrderNumber,
           JSON_VALUE(Doc, '$.CustomerName') AS Customer,
           Doc
    FROM
        OPENROWSET(
            BULK 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/json/',
            FORMAT = 'CSV',
            FIELDTERMINATOR ='0x0b',
            FIELDQUOTE = '0x0b',
            ROWTERMINATOR = '0x0b'
        ) WITH (Doc NVARCHAR(MAX)) as rows
    ```

1. Name your script **Sales JSON query** by clicking the properties tab icon, and publish it. Then close the script pane.

   ![Screenshot showing the steps](../images/DP500-1-33.png)

## Access external data in a database

So far, you've used the OPENROWSET function in a SELECT query to retrieve data from files in a data lake. The queries have been run in the context of the **master** database in your serverless SQL pool. This approach is fine for an initial exploration of the data, but if you plan to create more complex queries it may be more effective to use the *PolyBase* capability of Synapse SQL to create objects in a database that reference the external data location.

### Task-5: Create an external data source

By defining an external data source in a database, you can use it to reference the data lake location where the files are stored.

1. In Synapse Studio, on the **Develop** page, in the **+** menu, select **SQL script**.

   ![Screenshot showing the steps](../images/DP500-1-34.png)
   
1. In the new script pane, add the following code (replacing *datalakexxxxxxx* with the name of your data lake storage account as **datalake<inject key="DeploymentID" enableCopy="false"/>**) to create a new database and add an external data source to it.

    ```sql
    CREATE DATABASE Sales
      COLLATE Latin1_General_100_BIN2_UTF8;
    GO;

    Use Sales;
    GO;

    CREATE EXTERNAL DATA SOURCE sales_data WITH (
        LOCATION = 'https://datalakexxxxxxx.dfs.core.windows.net/files/sales/'
    );
    GO;
    ```

1. Modify the script properties to change its name to **Create Sales DB**, and publish it.

   ![Screenshot showing the steps](../images/DP500-1-35.png)
   
1. Ensure that the script is connected to the **Built-in** SQL pool and the **master** database, and then run it.

   ![Screenshot showing the steps](../images/DP500-1-36.png)

1. Switch back to the **Data** page and use the **&#8635;** button at the top right of Synapse Studio to refresh the page. Then view the **Workspace** tab in the **Data** pane, where a **SQL database** list is now displayed. Expand this list to verify that the **Sales** database has been created.

   ![Screenshot showing the steps](../images/DP500-1-37.png)
   
1. Expand the **Sales** database, its **External Resources** folder, and the **External data sources** folder under that to see the **sales_data** external data source you created.

   ![Screenshot showing the steps](../images/DP500-1-38.png)
   
1. In the **...** menu for the **Sales** database, select **New SQL script** > **Empty script**.

    ![Screenshot showing the steps](../images/DP500-1-39.png)
    
3.  Then in the new script pane, enter and run the following query:

    ```sql
    SELECT *
    FROM
        OPENROWSET(
            BULK 'csv/*.csv',
            DATA_SOURCE = 'sales_data',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0'
        ) AS orders
    ```
   
    ![Screenshot showing the steps](../images/DP500-1-40.png)
    
    The query uses the external data source to connect to the data lake, and the OPENROWSET function now only need to reference the relative path to the .csv files.
    
     ![Screenshot showing the steps](../images/DP500-1-41.png)

8. Modify the code as follows to query the parquet files using the data source and run the query.

    ```sql
    SELECT *
    FROM  
        OPENROWSET(
            BULK 'parquet/year=*/*.snappy.parquet',
            DATA_SOURCE = 'sales_data',
            FORMAT='PARQUET'
        ) AS orders
    WHERE orders.filepath(1) = '2019'
    ```
   ![Screenshot showing the steps](../images/DP500-1-42.png)
   
### Task-6: Create an external table

The external data source makes it easier to access the files in the data lake, but most data analysts using SQL are used to working with tables in a database. Fortunately, you can also define external file formats and external tables that encapsulate rowsets from files in database tables.

1. Replace the SQL code with the following statement to define an external data format for CSV files, and an external table that references the CSV files, and run it:

    ```sql
    CREATE EXTERNAL FILE FORMAT CsvFormat
        WITH (
            FORMAT_TYPE = DELIMITEDTEXT,
            FORMAT_OPTIONS(
            FIELD_TERMINATOR = ',',
            STRING_DELIMITER = '"'
            )
        );
    GO;

    CREATE EXTERNAL TABLE dbo.orders
    (
        SalesOrderNumber VARCHAR(10),
        SalesOrderLineNumber INT,
        OrderDate DATE,
        CustomerName VARCHAR(25),
        EmailAddress VARCHAR(50),
        Item VARCHAR(30),
        Quantity INT,
        UnitPrice DECIMAL(18,2),
        TaxAmount DECIMAL (18,2)
    )
    WITH
    (
        DATA_SOURCE =sales_data,
        LOCATION = 'csv/*.csv',
        FILE_FORMAT = CsvFormat
    );
    GO
    ```
    ![Screenshot showing the steps](../images/DP500-1-43.png)
    
1. Refresh and expand the **External tables** folder in the **Data** pane and confirm that a table named **dbo.orders** has been created in the **Sales** database.

   ![Screenshot showing the steps](../images/DP500-1-44.png)

4. In the **...** menu for the **dbo.orders** table, select **New SQL script** > **Select TOP 100 rows**.

   ![Screenshot showing the steps](../images/DP500-1-45.png)
   
6. Run the SELECT script that has been generated, and verify that it retrieves the first 100 rows of data from the table, which in turn references the files in the data lake.

## Task-7: Visualize query results

Now that you've explored various ways to query files in the data lake by using SQL queries, you can analyze the results of these queries to gain insights into the data. Often, insights are easier to uncover by visualizing the query results in a chart; which you can easily do by using the integrated charting functionality in the Synapse Studio query editor.

1. On the **Develop** page, create a new empty SQL query.

   ![Screenshot showing the steps](../images/DP500-1-46.png)
   
1. Ensure that the script is connected to the **Built-in** SQL pool and the **Sales** database.
   
1. Enter and run the following SQL code:

    ```sql
    SELECT YEAR(OrderDate) AS OrderYear,
           SUM((UnitPrice * Quantity) + TaxAmount) AS GrossRevenue
    FROM dbo.orders
    GROUP BY YEAR(OrderDate)
    ORDER BY OrderYear;
    ```
    
    ![Screenshot showing the steps](../images/DP500-1-47.png)
    
1. In the **Results** pane, select **Chart** and view the chart that is created for you; which should be a line chart.

1. Change the **Category column** to **OrderYear** so that the line chart shows the revenue trend over the three year period from 2019 to 2021:

    ![Screenshot showing the steps](../images/DP500-1-48.png)

1. Switch the **Chart type** to **Column** to see the yearly revenue as a column chart:

    ![Screenshot showing the steps](../images/DP500-1-49.png)

1. Experiment with the charting functionality in the query editor. It offers some basic charting capabilities that you can use while interactively exploring data, and you can save charts as images to include in reports. However, functionality is limited compared to enterprise data visualization tools such as Microsoft Power BI.

   > **Note**: **Congratulations!** You have successfully completed this task. Please validate your progress by clicking on **(...) icon** from upper right corner of lab guide section and switch to **Lab Validation** tab and then click on **Validate** button for the respective task.

1. **Congratulations** on completing the task! Now, it's time to validate it. Here are the steps:

   - Click the **(...) icon** located at the upper right corner of the lab guide section and navigate to the **Lab Validation** Page.
   - Hit the **Validate** button for the corresponding task.
   - If you receive a success message, you can proceed to the next task. If not, carefully read the error message and retry the step, following the instructions in the lab guide.
   - If you need any assistance, please contact us at [labs-support@spektrasystems.com](labs-support@spektrasystems.com).We are available 24/7 to help you out.
