def Query_Bigquery_DataWarehouse_Tiki(Tiki_Warehouse, Tiki_Staging):
    query = f"""
        DROP TABLE IF EXISTS `{Tiki_Warehouse}`;

        CREATE TABLE `{Tiki_Warehouse}` AS
        SELECT 
            _id,
            name_product,
            product_id,
            day_created,
            ARRAY_AGG(STRUCT(value, code)) AS brand_origin,
            Rating,
            Brand,
            Quantity,
            Price,
            Total_Price,
            category_name,
            category_id,
            Descriptions,
            return_and_exchange_policies,
            benefit
        FROM (
            SELECT DISTINCT 
                _id,
                Tiki_table.name AS name_product,
                current_seller.product_id AS product_id,
                CURRENT_DATE - day_ago_created AS day_created,
                attribute.value,
                attribute.code,
                Tiki_table.rating_average AS Rating,
                Tiki_table.current_seller.name AS Brand,
                Tiki_table.all_time_quantity_sold AS Quantity,
                Tiki_table.price AS Price,
                (Tiki_table.all_time_quantity_sold * Tiki_table.price) AS Total_Price,
                Tiki_table.categories.name AS category_name,
                Tiki_table.categories.id AS category_id,
                REGEXP_REPLACE(description, r'<[^>]*>', ' ') AS Descriptions,
                REGEXP_REPLACE(return_and_exchange_policy, r'<[^>]*>', ' ') AS return_and_exchange_policies,
                REGEXP_REPLACE(benefits[SAFE_OFFSET(0)].text, r'<[^>]*>', ' ') AS benefit
            FROM `{Tiki_Staging}` AS Tiki_table,
                UNNEST(Tiki_table.specifications) AS spec,
                UNNEST(spec.attributes) AS attribute
            WHERE Tiki_table.inventory_status = "available"
        )
        GROUP BY
            _id,
            name_product,
            product_id,
            day_created,
            Rating,
            Brand,
            Quantity,
            Price,
            Total_Price,
            category_name,
            category_id,
            Descriptions,
            return_and_exchange_policies,
            benefit
        ORDER BY category_id DESC;
    """
    return query

def Query_Bigquery_DataWarehouse_Newegg(Table_Newegg):
    query = f"""
        DROP TABLE IF EXISTS `project6-airflow.Data_db.Newegg_Warehouse`;
        CREATE TABLE `project6-airflow.Data_db.Newegg_Warehouse` AS
        SELECT Branding,
                Specs_data,
                Price,
                Rating 
        FROM `project6-airflow.Data_db.{Table_Newegg}`
        WHERE Price != 0;
    """
    return query

def Query_Update_Bigquery_Datamart_Tiki(Table_Datamart_Dic, Table_Warehouse_Dic):
    query = f"""
        MERGE `{Table_Datamart_Dic}` AS Datamart 
        USING `{Table_Warehouse_Dic}` AS Warehouse
        ON Datamart.product_id = Warehouse.product_id
        WHEN NOT MATCHED THEN
            INSERT(name_product,
                    product_id,
                    category_name,
                    Brand,
                    Rating,
                    Brand_origin,
                    Quantity,
                    Price)      
            VALUES(name_product,
                    product_id,
                    category_name,
                    Brand,
                    Rating,
                    Brand_origin,
                    Quantity,
                    Price)
    """
    return query

def Query_Create_Bigquery_Datamart_Tiki(Table_Tiki_Datamart_sample, Table_Warehouse_Dic):
    query = f"""
        CREATE TABLE `{Table_Tiki_Datamart_sample}` AS
        SELECT name_product,
                product_id,
                category_name, 
                Brand,
                Rating,
                Brand_origin,
                Quantity,
                Price
        FROM `{Table_Warehouse_Dic}`;
    """
    return query

def Query_Update_Bigquery_Datamart_Newegg(Table_Datamart_Dic, Table_Warehouse_Dic):
    query = f"""
        MERGE `{Table_Datamart_Dic}` AS Datamart 
        USING `{Table_Warehouse_Dic}` AS Warehouse
        ON Datamart.Specs_data = Warehouse.Specs_data
        WHEN NOT MATCHED THEN
            INSERT(Branding,
                    Specs_data,
                    Price,
                    Rating)   
            VALUES(Branding,
                    Specs_data,
                    Price,
                    Rating)
    """
    return query

def Bigquery_Create_Datamart_Newegg(Table_Newegg_Datamart_sample, Table_Warehouse_Dic):
    query = f"""
        DROP TABLE IF EXISTS `{Table_Newegg_Datamart_sample}`;
        CREATE TABLE `{Table_Newegg_Datamart_sample}` AS
        SELECT Branding,
                Specs_data,
                Price,
                Rating 
        FROM `{Table_Warehouse_Dic}`
        WHERE Price != 0;
    """
    return query
