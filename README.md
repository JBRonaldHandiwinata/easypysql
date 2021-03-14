# easypysql
Simplify python CRUD mysql using mysqlclient &amp; DBUtils 1.3

### SELECT single row
```bash
    es = EasySql()
    es.fetch_rows(sql=""" SELECT * FROM database_table """, None)
    es.closing()
```


### SELECT multiple rows
```bash
    valuecode = "XX1233"
    es = EasySql()
    es.fetch_rows(sql=""" SELECT * FROM database_table where valuecode=%s """, 
                  values(valuecode,), many=True)
    es.closing()
```

### INSERT single row
```bash
   es = EasySql()
   es.insert_rows(tbl="database_table", colandval=dict(column_name=column_value))
   es.dispose()
   es.closing()
```


### INSERT multiple rows
```bash
   es = EasySql()
   rows = [dict(column_name1=column_value1), dict(column_name2=column_value2)]
   es.insert_rows(tbl="database_table", colandval=rows)
   es.dispose()
   es.closing()
```

### UPDATE 
```bash
    es = EasySql()
    es.update_rows(tbl="database_table", 
                   dictset=dict(column_name_to_update=new_value), 
                   dictwhere=dict(where_column_name=where_column_value))
    es.dispose()
    es.close()
```

### Share it to multiple operations
```bash
  es = EasySql()
  i_select = es.fetch_rows(sql=""" SELECT * FROM database_table where valuecode=%s """, 
                           values(valuecode,), many=1)
                           
  i_insert = es.insert_rows(tbl="database_table", i_select)
  es.dispose()
  es.closing()
```

### Inherit to another class
```bash
   import easysql
   
   class AnotherClass(easysql.EasySql):
       DB_HOST = "mynewdbhost"
       DB_NAME = "newdbname"
       DB_USER = "myuser"
       DB_PWD = "mypassword"
  
  as = AnotherClass()
  as.fetch_rows(sql=""" SELECT * FROM database_table """, None)
  as.closing()
```

