CREATE OR REPLACE PROCEDURE DB.SCHEMA.PROC("FULL_TABLE_NAME" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS `

    var table_params = FULL_TABLE_NAME.split("__");

    if (table_params.length < 5) {
        return {error: "Table name does not have enough parameters."};
    }
    else if (table_params.length > 6) {
        return {error: "Table name has too many parameters."};
    }

    var array = table_params.slice(0,4);

    return array.map(col => col.toLowerCase());

`;
