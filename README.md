# sqldbcopy
Copies SQL data from one SQL Server to another

This little tool copies data from one SQL Server to another as long as both instances can be reached. It uses the SqlBulkCopy class in the background to really speed the transfer.

## Examples
### Example 1: Copy all tables in database TestDb from Server SQLA to SQLB using Windows Authentication
```
sqldbcopy "Data Source=SQLA;Initial Catalog=TestDb;Integrated Security=SSPI" "Data Source=SQLB;Initial Catalog=SQLB;Integrated Security=SSPI" *
```

### Example 2: Copy all tables synchronously in database TestDb from Server SQLA to SQLB using Windows Authentication
```
sqldbcopy "Data Source=SQLA;Initial Catalog=TestDb;Integrated Security=SSPI" "Data Source=SQLB;Initial Catalog=SQLB;Integrated Security=SSPI" * 1
```
