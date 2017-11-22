# sql2sql
A simple lightweight tool to perform ETL jobs between two SQL databases

## Installation

```bash
pip3 install --user sql2sql
```

## Usage
It's assumed that the SQL database driver is used with cursors.


### Basic Example

```python
# Setting up the db connections
import psycopg2
import cx_Oracle

oracle = cx_Oracle.connect("username", "password", "url/db")
psql = psycopg2.connect("dbname='dbname' user='username' host='url' password='password'")


# Actual usage
from sql2sql.objects import ETL

extract = "sELECT col1, col2 FROM some_table"
def transform(each_row):
    print(row[0], row[1])
    return row
load = "iNSERT INTO new_table(col1, col2) VALUES (%s, %S)"

ETL().from_conn(oracle).to_conn(psql).extract(extract).transform(transform).load(load).execute()
```

### Order of operation
The actual order of function chain does not matter, as it stores everything and performs it during `execute`. The order of operation is

* from_conn
* before_extract
* **extract**
* after_extract
* **transform**
* to_conn
* before_load
* **load**
* after_load

