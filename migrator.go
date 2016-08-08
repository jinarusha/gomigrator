package gomigrator

import (
	"fmt"
	"strconv"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/schema"
	"github.com/siddontang/go/log"
)

const (
	replaceSql string = "REPLACE INTO %s VALUES(%s)"
	deleteSql  string = "DELETE FROM %s WHERE %s "
)

type Migrator struct {
	MasterHost         string
	MasterPort         uint16
	MasterUser         string
	MasterDatabaseName string
	MasterPassword     string

	SlaveHost         string
	SlavePort         uint16
	SlaveDatabaseName string
	SlaveUser         string
	SlavePassword     string

	BinlogFilename string
	BinlogPosition uint32

	masterConn *client.Conn
	slaveConn  *client.Conn
}

func (m *Migrator) initSlaveDBConn() {
	conn, err := client.Connect(m.SlaveHost+":"+strconv.Itoa(int(m.SlavePort)), m.SlaveUser, m.SlavePassword, m.SlaveDatabaseName)
	if err != nil {
		panic(err)
	}
	m.slaveConn = conn
	log.Infof("Connection to target DB made")
}

func (m *Migrator) close() {
	if m.masterConn != nil {
		log.Infof("Closing connection to origin DB")
		m.masterConn.Close()
	}
	if m.slaveConn != nil {
		log.Infof("Closing connection to target DB")
		m.slaveConn.Close()
	}
}

func (m *Migrator) StartSync(serverId uint32, serverType string) {
	defer m.close()

	m.initSlaveDBConn()

	syncer := replication.NewBinlogSyncer(serverId, serverType)
	syncer.RegisterSlave(m.MasterHost, m.MasterPort, m.MasterUser, m.MasterPassword)

	pos := mysql.Position{m.BinlogFilename, m.BinlogPosition}
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		panic(err)
	}

	for {
		ev, err := streamer.GetEvent()
		if err != nil {
			panic(err)
		}

		switch e := ev.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(e.NextLogName)
			pos.Pos = uint32(e.Position)
			log.Infof("Rotating binlog to %v", pos)
		default:
			// handleRowsEvent returns error when event is none of insert/update/delete
			// So you can ignore errors here if all you care about is insert/update/delete
			if err := m.handleRowsEvent(ev); err != nil {
				continue
			}
		}
	}
}

func (m *Migrator) handleRowsEvent(e *replication.BinlogEvent) error {

	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		ev := e.Event.(*replication.RowsEvent)
		table := string(ev.Table.Table)
		m.upsertRow(table, ev.Rows[0], "insert")
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		ev := e.Event.(*replication.RowsEvent)
		table := string(ev.Table.Table)
		m.deleteRow(table, ev.Rows[0])
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		ev := e.Event.(*replication.RowsEvent)
		table := string(ev.Table.Table)

		primaryKeyIdx := m.getPrimaryKeyIdx(table)
		beforePk := ev.Rows[0][primaryKeyIdx]
		afterPk := ev.Rows[1][primaryKeyIdx]
		if beforePk != afterPk {
			m.deleteRow(table, ev.Rows[0])
		}
		m.upsertRow(table, ev.Rows[1], "update")
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}

	return nil
}

func (m *Migrator) getPrimaryKeyIdx(table string) int {
	t, err := schema.NewTable(m.slaveConn, m.slaveConn.GetDB(), table)
	if err != nil {
		panic(err)
	}
	return t.PKColumns[0]
}

func (m *Migrator) deleteRow(table string, params []interface{}) (*mysql.Result, error) {
	t, err := schema.NewTable(m.slaveConn, m.slaveConn.GetDB(), table)
	if err != nil {
		log.Error("deleteRow: err: ", err)
		return nil, err
	}
	colSize := len(params)
	paramStr := ""

	paramsWithoutNil := []interface{}{}

	for idx := 0; idx < len(params); idx++ {
		colName := t.Columns[idx].Name

		if params[idx] == nil {
			paramStr = paramStr + colName + " IS NULL"
		} else {
			paramStr = paramStr + colName + "=?"
			paramsWithoutNil = append(paramsWithoutNil, params[idx])
		}

		if colSize != 1 && idx != colSize-1 {
			paramStr += " AND "
		}
	}

	query := fmt.Sprintf(deleteSql, table, paramStr)
	result, err := m.execute(query, paramsWithoutNil)
	if err != nil {
		log.Error("deleteRow: err: ", err)
	} else {
		log.Info("Deleting from ", table)
	}
	return result, err
}

func (m *Migrator) upsertRow(table string, params []interface{}, action string) (*mysql.Result, error) {
	colSize := len(params)
	paramStr := ""
	for idx := 0; idx < colSize; idx++ {
		paramStr += "?"
		if colSize != 1 && idx != colSize-1 {
			paramStr += ","
		}
	}

	query := fmt.Sprintf(replaceSql, table, paramStr)
	result, err := m.execute(query, params)
	if err != nil {
		log.Error("deleteRow: err: ", err)
	} else {
		if action == "insert" {
			log.Info("Inserting into ", table)
		} else if action == "update" {
			log.Info("Updating in ", table)
		}
	}
	return result, err
}

func (m *Migrator) execute(query string, params []interface{}) (*mysql.Result, error) {
	stmt, err := m.slaveConn.Prepare(query)
	if err != nil {
		log.Error("Failed to execute: ", query, params)
	}
	defer stmt.Close()
	return stmt.Execute(params...)
}
