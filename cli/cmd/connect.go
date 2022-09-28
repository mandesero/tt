package cmd

import (
	"fmt"
	"regexp"
	"syscall"

	"github.com/apex/log"
	"github.com/spf13/cobra"
	"github.com/tarantool/tt/cli/cmdcontext"
	"github.com/tarantool/tt/cli/config"
	"github.com/tarantool/tt/cli/configure"
	"github.com/tarantool/tt/cli/connect"
	"github.com/tarantool/tt/cli/modules"
	"github.com/tarantool/tt/cli/running"
	"golang.org/x/crypto/ssh/terminal"
)

var (
	connectUser        string
	connectPassword    string
	connectFile        string
	connectLanguage    string
	connectInteractive bool
)

// NewConnectCmd creates connect command.
func NewConnectCmd() *cobra.Command {
	var connectCmd = &cobra.Command{
		Use: "connect (<APP_NAME> | <APP_NAME:INSTANCE_NAME> | <URI>)" +
			" [<FILE> | <COMMAND>] [flags]\n" +
			"  COMMAND | tt connect (<APP_NAME> | <APP_NAME:INSTANCE_NAME> | <URI>) [flags]",
		Short: "Connect to the tarantool instance",
		Run: func(cmd *cobra.Command, args []string) {
			cmdCtx.CommandName = cmd.Name()
			err := modules.RunCmd(&cmdCtx, cmd.Name(), &modulesInfo, internalConnectModule, args)
			if err != nil {
				log.Fatalf(err.Error())
			}
		},
	}

	connectCmd.Flags().StringVarP(&connectUser, "username", "u", "", "username")
	connectCmd.Flags().StringVarP(&connectPassword, "password", "p", "", "password")
	connectCmd.Flags().StringVarP(&connectFile, "file", "f", "",
		`file to read the script for evaluation. "-" - read the script from stdin`)
	connectCmd.Flags().StringVarP(&connectLanguage, "language", "l",
		connect.DefaultLanguage.String(), `language: lua or sql`)
	connectCmd.Flags().BoolVarP(&connectInteractive, "interactive", "i",
		false, `enter interactive mode after executing 'FILE'`)

	return connectCmd
}

// isURI returns true if a string is a valid URI.
func isURI(str string) bool {
	// tcp://host:port
	// host:port
	tcpReStr := `(tcp://)?([\w\\.-]+:\d+)`
	// unix://../path
	// unix:///path
	// unix://path
	unixReStr := `unix://[./]*[^\./]+.*`
	// ./path
	// /path
	pathReStr := `\.?/[^\./].*`

	uriReStr := "^((" + tcpReStr + ")|(" + unixReStr + ")|(" + pathReStr + "))$"
	uriRe := regexp.MustCompile(uriReStr)
	return uriRe.Match([]byte(str))
}

// resolveInstAddr tries to resolve the first passed argument as an instance
// name to replace it with a control socket or as a URI.
func resolveInstAddr(cmdCtx *cmdcontext.CmdCtx, cliOpts *config.CliOpts,
	args []string) ([]string, error) {
	newArgs := args

	// FillCtx returns error if no instances found.
	if fillErr := running.FillCtx(cliOpts, cmdCtx, args); fillErr == nil {
		if len(cmdCtx.Running) > 1 {
			return newArgs, fmt.Errorf("specify instance name")
		}
		if cmdCtx.Connect.Username != "" || cmdCtx.Connect.Password != "" {
			return newArgs, fmt.Errorf("username and password are not supported" +
				" with a connection via a control socket")
		}
		newArgs[0] = cmdCtx.Running[0].ConsoleSocket
		return newArgs, nil
	} else {
		if isURI(newArgs[0]) {
			return newArgs, nil
		}
		return newArgs, fillErr
	}
}

// internalConnectModule is a default connect module.
func internalConnectModule(cmdCtx *cmdcontext.CmdCtx, args []string) error {
	argsLen := len(args)
	if argsLen != 1 {
		return fmt.Errorf("Incorrect combination of command parameters")
	}

	cliOpts, err := configure.GetCliOpts(cmdCtx.Cli.ConfigPath)
	if err != nil {
		return err
	}

	cmdCtx.Connect.Username = connectUser
	cmdCtx.Connect.Password = connectPassword
	cmdCtx.Connect.SrcFile = connectFile
	cmdCtx.Connect.Language = connectLanguage
	cmdCtx.Connect.Interactive = connectInteractive

	newArgs, err := resolveInstAddr(cmdCtx, cliOpts, args)
	if err != nil {
		return err
	}

	if connectFile != "" {
		res, err := connect.Eval(cmdCtx, newArgs)
		if err != nil {
			return err
		}
		// "Println" is used instead of "log..." to print the result without
		// any decoration.
		fmt.Println(string(res))
		if !connectInteractive || !terminal.IsTerminal(syscall.Stdin) {
			return nil
		}
	}

	if terminal.IsTerminal(syscall.Stdin) {
		log.Info("Connecting to the instance...")
	}
	if err := connect.Connect(cmdCtx, newArgs); err != nil {
		return err
	}

	return nil
}
