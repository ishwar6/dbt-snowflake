CREATE OR REPLACE PROCEDURE  T(FULL_TABLE_NAME VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var table_params;
    var schema_name = "";

    if (FULL_TABLE_NAME.includes(".")) {
        table_params = FULL_TABLE_NAME.split(".");
        if (table_params.length != 2) {
            return {error: "Table name does not have a schema."};
        }
        schema_name = table_params[0].replace(/"/g,"");
        table_params = table_params[1].replace(/"/g,"");
    }
    else {
        table_params = FULL_TABLE_NAME.replace(/"/g,"");
    }

    var params = table_params.split("__");

    if (params == null || params.length < 3) {
        return {error: "Table name does not have enough parameters."};
    }
    else if (params.length > 6) {
        return {error: "Table name has too many parameters."};
    }

    var array = params.slice(0,3);

    return [schema_name].concat(array.map(col => col.toLowerCase()));
';
