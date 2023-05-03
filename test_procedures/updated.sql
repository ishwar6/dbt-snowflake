CREATE OR REPLACE PROCEDURE TEST(FULL_TABLE_NAME VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var table_params;

    if (FULL_TABLE_NAME.includes(".")) {
        table_params = FULL_TABLE_NAME.split(".");
        if (table_params.length != 2) {
            return {error: "Table name does not have a schema."};
        }
        var schema_name = table_params[0];
        table_params = table_params[1];
    }
    else {
        table_params = FULL_TABLE_NAME;
        var schema_name = "";
    }

    var params = table_params.match(/"(\d*?)__(\d*?)__([A-Za-z0-9_]*)__([A-Za-z0-9_]*)__([A-Za-z0-9_]*)"/);

    if (params == null) {
        params = table_params.match(/(\d*?)__(\d*?)__([A-Za-z0-9_]*)__([A-Za-z0-9_]*)__([A-Za-z0-9_]*)/);
    }

    if (params == null || params.length < 5) {
        return {error: "Table name does not have enough parameters."};
    }
    

    var array = params.slice(2,5);

    return [schema_name].concat(array.map(col => col.toLowerCase()));
';

v2_sqlText = "SELECT 'SELECT ' || LISTAGG(COALESCE(" + qry_col + ", ''n/a'') || ' AS ' || output, ', ') || ' FROM ' || '" + 
  FULL_TABLE_NAME.toString() + "' || ' GROUP BY ' || " + group_by_cols + " || '\\'' AS query from  test.test_schmea.learn`;
  
  var v2_sqlText = `SELECT 'SELECT ' || LISTAGG(COALESCE(${qry_col}, 'n/a') || ' AS ' || output, ', ') || ' FROM ${FULL_TABLE_NAME} GROUP BY ${modified_group_by_cols}' 
  AS query FROM  test.test_schmea.learn`;

