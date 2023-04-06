CREATE OR REPLACE PROCEDURE GET_CCMB_ORDERS(DATABASE_NAME VARCHAR, SCHEMA_NAME VARCHAR, CLIENT_CART_ID VARCHAR, CLIENT_STORE_ID VARCHAR, CLIENT_NAME VARCHAR)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
    var columnNames = [];
    var query = `SELECT column_name FROM ${SCHEMA_NAME}.information_schema.columns WHERE table_schema = '${DATABASE_NAME}' AND table_name = '${CLIENT_CART_ID}__${CLIENT_STORE_ID}__${CLIENT_NAME}__ONLINESTORE__orders'`;
    var stmt = snowflake.createStatement({sqlText: query});
    var resultSet = stmt.execute();
    while (resultSet.next()) {
        columnNames.push(resultSet.getColumnValue(1));
    }
    
    var columns = columnNames.join(", ");
    var query = `WITH AVERAGE_FEES as (SELECT ID, SUBTOTAL, GRAND_TOTAL, SUM(ifnull(TAX_TOTAL,0) + ifnull(HANDLING_TOTAL,0) + ifnull(DISCOUNTED_SHIPPING_TOTAL,0) + ifnull(ADDITIONAL_FEES,0) - ifnull(DISCOUNT_TOTAL,0)) as TOTAL_FEES
                FROM ${SCHEMA_NAME}.${DATABASE_NAME}.${CLIENT_CART_ID}__${CLIENT_STORE_ID}__${CLIENT_NAME}__ONLINESTORE__orders
                GROUP BY ID, SUBTOTAL, GRAND_TOTAL),
                ITEM_COUNT as (SELECT ORDER_ID, SUM(QUANTITY)as ITEM_COUNT
                FROM ${SCHEMA_NAME}.${DATABASE_NAME}.${CLIENT_CART_ID}__${CLIENT_STORE_ID}__${CLIENT_NAME}__ONLINESTORE__order_items
                GROUP BY ORDER_ID),
                CCMB_ORDERS AS(
                    SELECT * FROM(
                        SELECT *,row_number() over(partition by id order by created_at desc) rn from(
                            SELECT a.postal_code as zipcode, b.${columns} from ${SCHEMA_NAME}.${DATABASE_NAME}.${CLIENT_CART_ID}__${CLIENT_STORE_ID}__${CLIENT_NAME}__ONLINESTORE__orders b
                            LEFT JOIN ${SCHEMA_NAME}.${DATABASE_NAME}.${CLIENT_CART_ID}__${CLIENT_STORE_ID}__${CLIENT_NAME}__ONLINESTORE__addresses a ON
                            b.CUSTOMER_ID = a.CUSTOMER_ID
                        )
                    )where rn =1
                )
                SELECT * FROM CCMB_ORDERS`;
    
    var stmt = snowflake.createStatement({sqlText: query});
    var resultSet = stmt.execute();
    resultSet.next();
    var result = resultSet.getColumnValue(1);
    
    return result;
$$;


CALL GET_CCMB_ORDERS('MYST', 'MYDB', '2', '63128', 'TEST');
