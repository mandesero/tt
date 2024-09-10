package downgrade

import (
	_ "embed"
	"errors"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/tarantool/tt/cli/connector"
	"github.com/tarantool/tt/cli/replicaset"
	"github.com/tarantool/tt/cli/running"
	"github.com/tarantool/tt/cli/upgrade"
)

var DowngradeVersion string

//go:embed lua/downgrade.lua
var downgradeLuaScript string

type SyncInfo = upgrade.SyncInfo

func internalDowngrade(conn connector.Connector) (SyncInfo, error) {
	var downgradeInfo SyncInfo
	res, err := conn.Eval(downgradeLuaScript, []interface{}{DowngradeVersion}, connector.RequestOpts{})
	if err != nil {
		return downgradeInfo, err
	}
	if err := mapstructure.Decode(res[0], &downgradeInfo); err != nil {
		return downgradeInfo, err
	}
	if downgradeInfo.Err != nil {
		return downgradeInfo, errors.New(*downgradeInfo.Err)
	}
	return downgradeInfo, nil
}

func Downgrade(replicasets replicaset.Replicasets, pendingReplicasetAliases []string,
	lsnTimeout int) error {
	allowedReplicasets, err := upgrade.GetAllowedReplicasets(replicasets,
		pendingReplicasetAliases)
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
		masterUpgradeInfo, err := internalDowngrade(conn)
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
			err = upgrade.WaitLSN(conn, masterUpgradeInfo, lsnTimeout)
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s]: LSN wait timeout: error waiting LSN %d "+
					"in vclock component %d on %s: time quota %d seconds "+
					"exceeded", rs.Alias, masterUpgradeInfo.LSN,
					masterUpgradeInfo.IID, fullInstanceName, lsnTimeout)
			}
			_, err = internalDowngrade(conn)
			if err != nil {
				printReplicasetStatus(rs.Alias, "error")
				return fmt.Errorf("[%s][%s]: %s", rs.Alias, fullInstanceName, err)
			}
		}
		printReplicasetStatus(rs.Alias, "ok")
	}
	return nil
}
