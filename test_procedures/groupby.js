 
v0a_sqlText = `SELECT LISTAGG(` + rf_col + ` || ' AS ' || output, ', ') as cols
               FROM test.schema.test
               WHERE LOWER(data_type) != 'numeric'`;

try {
  v0a_stmt = snowflake.createStatement({sqlText: v0a_sqlText});
  t0a = v0a_stmt.execute();
  t0a.next();
}
catch (err) {
  return 'There is no template for this report yet';
}

var non_numeric_cols = t0a.getColumnValue('COLS').toString();

// Query to grab numeric columns with SUM()
v0b_sqlText = `SELECT LISTAGG('SUM(' || ` + rf_col + ` || ') AS ' || output, ', ') as cols
               
               test.schema.test
               WHERE LOWER(data_type) = 'numeric'`;

try {
  v0b_stmt = snowflake.createStatement({sqlText: v0b_sqlText});
  t0b = v0b_stmt.execute();
  t0b.next();
}
catch (err) {
  return 'There is no template for this report yet';
}

var numeric_cols = t0b.getColumnValue('COLS').toString();

 
var combined_cols = non_numeric_cols + ', ' + numeric_cols;
 
v2_sqlText = `SELECT 'SELECT ' || '` + combined_cols + `' || ' FROM ' || '` + FULL_TABLE_NAME.toString() + `' || ' GROUP BY ` + group_by_cols + `\\'' as query`;

 
