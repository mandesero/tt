package upgrade

import (
	_ "embed"
	"errors"
	"fmt"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/tarantool/tt/cli/connector"
	"github.com/tarantool/tt/cli/replicaset"
	"github.com/tarantool/tt/cli/running"
)

//go:embed lua/upgrade.lua
var upgradeLuaScript string

type SyncInfo struct {
	LSN uint32  `mapstructure:"lsn"`
	IID uint32  `mapstructure:"iid"`
	Err *string `mapstructure:"err"`
}

func internalUpgrade(conn connector.Connector) (SyncInfo, error) {
	var upgradeInfo SyncInfo
	res, err := conn.Eval(upgradeLuaScript, []any{}, connector.RequestOpts{})
	if err != nil {
		return upgradeInfo, err
	}
	if err := mapstructure.Decode(res[0], &upgradeInfo); err != nil {
		return upgradeInfo, err
	}
	if upgradeInfo.Err != nil {
		return upgradeInfo, errors.New(*upgradeInfo.Err)
	}
	return upgradeInfo, nil
}

func WaitLSN(conn connector.Connector, masterUpgradeInfo SyncInfo, timeout int) error {
	for i := 0; i < timeout; i++ {
		var res []interface{}
		res, err := conn.Eval(
			fmt.Sprintf("return box.info.vclock[%d]", masterUpgradeInfo.IID),
			[]any{}, connector.RequestOpts{})
		if err != nil || len(res) == 0 {
			return err
		}
		var lsn uint32
		switch v := res[0].(type) {
		case uint16:
			lsn = uint32(v)
		case uint32:
			lsn = v
		}
		if lsn >= masterUpgradeInfo.LSN {
			return nil
		}
		time.Sleep(time.Second)
	}
	return errors.New("")
}

func GetAllowedReplicasets(allReplicasets replicaset.Replicasets,
	pendingReplicasetAliases []string) ([]replicaset.Replicaset, error) {
	if len(pendingReplicasetAliases) == 0 {
		return allReplicasets.Replicasets, nil
	}

	replicasetMap := make(map[string]replicaset.Replicaset)
	for _, rs := range allReplicasets.Replicasets {
		replicasetMap[rs.Alias] = rs
	}

	var allowedReplicasets []replicaset.Replicaset

	for _, alias := range pendingReplicasetAliases {
		replicaset, exists := replicasetMap[alias]
		if !exists {
			return nil, fmt.Errorf("replicaset with alias %q doesn't exist", alias)
		}
		allowedReplicasets = append(allowedReplicasets, replicaset)
	}

	return allowedReplicasets, nil
}

func Upgrade(replicasets replicaset.Replicasets, pendingReplicasetAliases []string,
	lsnTimeout int) error {
	allowedReplicasets, err := GetAllowedReplicasets(replicasets, pendingReplicasetAliases)
	if err != nil {
		return err
	}

	printReplicasetStatus := func(alias, status string) {
		fmt.Printf("• %s: %s\n", alias, status)
	}

	for _, rs := range allowedReplicasets {
		var masterRun *running.InstanceCtx = nil
		var replicRun []running.InstanceCtx

		for _, inst := range rs.Instances {
			run := inst.InstanceCtx
			fullInstanceName := running.GetAppInstanceName(run)
			conn, err := connector.Connect(connector.ConnectOpts{
				Network: "unix",
				Address: run.ConsoleSocket,
			})
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s][%s]: %s", rs.Alias, fullInstanceName, err)
			}

			res, err := conn.Eval(
				"return (type(box.cfg) == 'function') or box.info.ro",
				[]any{}, connector.RequestOpts{})
			if err != nil || len(res) == 0 {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s][%s]: %s", rs.Alias, fullInstanceName, err)
			}

			if !res[0].(bool) {
				if masterRun != nil {
					printReplicasetStatus(rs.Alias, "error")
					return fmt.Errorf("[%s]: %s and %s are both masters",
						rs.Alias, running.GetAppInstanceName(*masterRun),
						fullInstanceName)
				}
				masterRun = &run
			} else {
				replicRun = append(replicRun, run)
			}
		}
		if masterRun == nil {
			printReplicasetStatus(rs.Alias, "error")
			return fmt.Errorf("[%s]: has not master instance", rs.Alias)
		}
		var conn connector.Connector
		conn, err = connector.Connect(connector.ConnectOpts{
			Network: "unix",
			Address: masterRun.ConsoleSocket,
		})

		if err != nil {
			printReplicasetStatus(rs.Alias, "error")
			return fmt.Errorf("[%s][%s]: %s", rs.Alias,
				running.GetAppInstanceName(*masterRun), err)
		}
		masterUpgradeInfo, err := internalUpgrade(conn)
		if err != nil {
			printReplicasetStatus(rs.Alias, "error")
			return fmt.Errorf("[%s][%s]: %s", rs.Alias,
				running.GetAppInstanceName(*masterRun), err)
		}
		for _, run := range replicRun {
			fullInstanceName := running.GetAppInstanceName(run)
			conn, err = connector.Connect(connector.ConnectOpts{
				Network: "unix",
				Address: run.ConsoleSocket,
			})
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s][%s]: %s", rs.Alias, fullInstanceName, err)
			}
			err = WaitLSN(conn, masterUpgradeInfo, lsnTimeout)
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s]: LSN wait timeout: error waiting LSN %d "+
					"in vclock component %d on %s: time quota %d seconds "+
					"exceeded", rs.Alias, masterUpgradeInfo.LSN,
					masterUpgradeInfo.IID, fullInstanceName, lsnTimeout)
			}
			_, err = internalUpgrade(conn)
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s][%s]: %s", rs.Alias, fullInstanceName, err)
			}
		}
		printReplicasetStatus(rs.Alias, "ok")
	}
	return nil
}
