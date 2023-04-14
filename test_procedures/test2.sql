CREATE OR REPLACE PROCEDURE TEST2(TABLE_NAME VARCHAR)
RETURNS TABLE
LANGUAGE JAVASCRIPT
AS
$$
    var query = `
        CALL NAME_EXTRACT('${TABLE_NAME}')
    `;
    
    var stmt = snowflake.createStatement({
        sqlText: query
    });

    var rs = stmt.execute();

    var table_params = rs.next();

    if ('error' in table_params) {
        return table_params;
    }
    
    var schema_name = table_params[0];

    var database_name = table_params[1];

    var table_name_prefix = table_params[2];

    var client_id = table_params[3];

    var client_name = '';

    if (table_params.length == 5) {
        client_name = table_params[4];
    }
    else if (table_params.length == 6) {
        client_name = table_params[2];
    }
    else {
        return {error: "Table name does not have the correct number of parameters."};
    }

    
