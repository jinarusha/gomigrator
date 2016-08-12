gomigrator is a simple replication transmitter built using [go-mysql](https://github.com/siddontang/go-mysql).
Basically, the library reads binlog stream from master DB, parses data, and then does insert/update/delete operation on target database which can either be a replica or another master.

You might find this project useful for doing database migration when it's not possible to set replica facing external master.

## Example:
```go
package main

import "github.com/jinarusha/gomigrator"

func main() {

	migrator := gomigrator.Migrator{
		MasterHost:         "masterDBHostUrl",
		MasterPort:         3306,
		MasterDatabaseName: "masterDatabaseName",
		MasterUser:         "masterUser",
		MasterPassword:     "masterPassword",

		SlaveHost:         "targetDBHostUrl",
		SlavePort:         3306,
		SlaveDatabaseName: "targetDatabaseName",
		SlaveUser:         "targetUser",
		SlavePassword:     "targetPassword",

		BinlogFilename: "mariadb-bin.000478",
		BinlogPosition: 16287,
		
		// If it's too large, you might run out of memory
		MaxQueueSize: 500000,
	}

  // for MySQL, set "mariadb" -> "mysql"
  	var slaveServerId uint32 = 100
	migrator.StartSync(slaveServerId, "mariadb")
}
```
