package cmd

import (
	"github.com/spf13/cobra"
	"github.com/tarantool/tt/cli/cmd/internal"
	"github.com/tarantool/tt/cli/cmdcontext"
	"github.com/tarantool/tt/cli/modules"
	"github.com/tarantool/tt/cli/replicaset"
	replicasetcmd "github.com/tarantool/tt/cli/replicaset/cmd"
	"github.com/tarantool/tt/cli/running"
	"github.com/tarantool/tt/cli/upgrade"
	"github.com/tarantool/tt/cli/util"
)

var (
	Timeout                  *int
	pendingReplicasetAliases *[]string
)

func NewUpgradeCmd() *cobra.Command {
	var upgradeCmd = &cobra.Command{
		Use:   "upgrade [<APP_NAME>]",
		Short: "upgrade tarantool schema",
		Example: `tt upgrade                            - Upgrade all active replicasets
  tt upgrade <APP_NAME>                 - Upgrade replicasets of the specified app <APP_NAME>
  tt upgrade <APP_NAME> -r <RS_NAME>    - Upgrade specific replicaset <RS_NAME> of app <APP_NAME>`,
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx.CommandName = cmd.Name()
			err := modules.RunCmd(&cmdCtx, cmd.CommandPath(), &modulesInfo,
				upgradeReplicasets, args)
			util.HandleCmdErr(cmd, err)
		},
		ValidArgsFunction: func(
			cmd *cobra.Command,
			args []string,
			toComplete string) ([]string, cobra.ShellCompDirective) {
			return internal.ValidArgsFunction(
				cliOpts, &cmdCtx, cmd, toComplete,
				running.ExtractAppNames,
				running.ExtractInstanceNames)
		},
	}

	pendingReplicasetAliases = upgradeCmd.Flags().StringArrayP("replicaset", "r",
		[]string{}, "specify the replicaset name(s) to upgrade")

	Timeout = upgradeCmd.Flags().IntP("timeout", "t", 5,
		"timeout for waiting the LSN synchronization (in seconds)")

	return upgradeCmd
}

func prepareReplicasets(cmdCtx *cmdcontext.CmdCtx, args []string) (replicaset.Replicasets, error) {
	var ctx replicasetCtx
	if err := replicasetFillCtx(cmdCtx, &ctx, args, false); err != nil {
		return replicaset.Replicasets{}, err
	}
	if ctx.IsInstanceConnect {
		defer ctx.Conn.Close()
	}
	statusCtx := replicasetcmd.StatusCtx{
		IsApplication: ctx.IsApplication,
		RunningCtx:    ctx.RunningCtx,
		Conn:          ctx.Conn,
		Orchestrator:  ctx.Orchestrator,
	}

	replicasets, err := replicasetcmd.GetReplicasets(statusCtx)
	return replicasets, err
}

func upgradeReplicasets(cmdCtx *cmdcontext.CmdCtx, args []string) error {
	if !isConfigExist(cmdCtx) {
		return errNoConfig
	}

	replicasets, err := prepareReplicasets(cmdCtx, args)
	if err != nil {
		return err
	}

	return upgrade.Upgrade(replicasets, *pendingReplicasetAliases, *Timeout)
}
