package cmd

import (
	"errors"
	"os"
	"regexp"

	"github.com/spf13/cobra"
	"github.com/tarantool/tt/cli/cmd/internal"
	"github.com/tarantool/tt/cli/cmdcontext"
	"github.com/tarantool/tt/cli/downgrade"
	"github.com/tarantool/tt/cli/modules"
	"github.com/tarantool/tt/cli/running"
	"github.com/tarantool/tt/cli/util"
)

func NewDowngradeCmd() *cobra.Command {
	var versionPattern = regexp.MustCompile(`^\d+\.\d+\.\d+$`)
	var downgradeCmd = &cobra.Command{
		Use:   "downgrade [<APP_NAME>] --version <x.x.x>",
		Short: "downgrade tarantool schema",
		Run: func(cmd *cobra.Command, args []string) {
			if downgrade.DowngradeVersion == "" {
				err := errors.New("need to specify the version to downgrade " +
					"use --version (-v) option")
				util.HandleCmdErr(cmd, err)
				os.Exit(1)
			} else if !versionPattern.MatchString(downgrade.DowngradeVersion) {
				err := errors.New("--version (-v) must be in the format " +
					"'x.x.x', where x is a number")
				util.HandleCmdErr(cmd, err)
				os.Exit(1)
			}
			cmdCtx.CommandName = cmd.Name()
			err := modules.RunCmd(&cmdCtx, cmd.CommandPath(), &modulesInfo,
				downgradeReplicasets, args)
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

	pendingReplicasetAliases = downgradeCmd.Flags().StringArrayP("replicaset", "r",
		[]string{}, "specify the replicaset name(s) to downgrade")

	Timeout = downgradeCmd.Flags().IntP("timeout", "t", 5,
		"timeout for waiting the LSN synchronization (in seconds)")

	downgradeCmd.Flags().StringVarP(&downgrade.DowngradeVersion, "version", "v",
		"", "version to downgrade")

	return downgradeCmd
}

func downgradeReplicasets(cmdCtx *cmdcontext.CmdCtx, args []string) error {
	if !isConfigExist(cmdCtx) {
		return errNoConfig
	}

	replicasets, err := prepareReplicasets(cmdCtx, args)
	if err != nil {
		return err
	}

	return downgrade.Downgrade(replicasets, *pendingReplicasetAliases, *Timeout)
}
