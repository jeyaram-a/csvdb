# csvdb
Sql Interface for csv

__Work in progress. Will update README once it takes decent shape__

## Usage
```shell
csvdb path_to_file select_query
# example
csvdb "/home/j/development/csvdb/test.csv" "select * where a=a1, b!=b2"
```

## Supported operations
* where
    * col = "val" (!=, >, >=, <, <=). All are done as string comparisons. No type inference / type casting support yet.
    * logical operators. AND / OR (where a=a1 or a=a2)
* order by
    * ASC / DESC. 
    * assumes asc if not explicity mentioned
    * multiple order bys. (order by a, b)


## Not yet supported. But will be soon
* Grouping by + Aggregate Operations
* Limit
* Pattern matching where clauses (LIKE)
* Type casting 

## More ambitious goals
* Streaming output
* Indexable fields for larger files. Can greatly improve performance for subsequent queries. (Will make it interactive)