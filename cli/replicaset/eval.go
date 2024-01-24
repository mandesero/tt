package replicaset

import (
	"fmt"

	"github.com/apex/log"

	"github.com/tarantool/tt/cli/connector"
	"github.com/tarantool/tt/cli/running"
)

// InstanceEvaler is the interface that wraps Eval method for an instance.
type InstanceEvaler interface {
	// Eval could return true or an error to stop execution.
	Eval(instance running.InstanceCtx, evaler connector.Evaler) (bool, error)
}

// InstanceEvalFunc helps to use a function as the InstanceEvaler interface.
type InstanceEvalFunc func(instance running.InstanceCtx,
	evaler connector.Evaler) (bool, error)

// Eval helps to satisfy the InstanceEvaler insterface.
func (fun InstanceEvalFunc) Eval(instance running.InstanceCtx,
	evaler connector.Evaler) (bool, error) {
	return fun(instance, evaler)
}

// EvalForeach calls evaler for each instance.
func EvalForeach(instances []running.InstanceCtx, ievaler InstanceEvaler) error {
	return evalForeach(instances, ievaler, false)
}

// EvalForeachAlive calls evaler for each connectable instance.
func EvalForeachAlive(instances []running.InstanceCtx, ievaler InstanceEvaler) error {
	return evalForeach(instances, ievaler, true)
}

// EvalAny calls evaler once for one connectable instance.
func EvalAny(instances []running.InstanceCtx, ievaler InstanceEvaler) error {
	return EvalForeachAlive(instances, InstanceEvalFunc(
		func(instance running.InstanceCtx, evaler connector.Evaler) (bool, error) {
			_, err := ievaler.Eval(instance, evaler)
			// Always return true to stop execution on the first instance.
			return true, err
		}))
}

// evalForeach is an internal implementation of iteration over instances with
// an evaler object.
func evalForeach(instances []running.InstanceCtx,
	ievaler InstanceEvaler, skipConnectError bool) error {
	if len(instances) == 0 {
		return fmt.Errorf("no instances to connect")
	}

	connected := 0
	for _, instance := range instances {
		conn, err := connector.Connect(connector.ConnectOpts{
			Network: "unix",
			Address: instance.ConsoleSocket,
		})
		if err != nil {
			if !skipConnectError {
				return fmt.Errorf("failed to connect to '%s:%s': %w",
					instance.AppName, instance.InstName, err)
			} else {
				log.Debugf("failed to connect to '%s:%s': %s",
					instance.AppName, instance.InstName, err)
				continue
			}
		}

		connected++
		done, err := ievaler.Eval(instance, conn)
		conn.Close()

		if err != nil {
			return err
		}
		if done {
			break
		}
	}
	if connected == 0 {
		return fmt.Errorf("failed to connect to any instance")
	}
	return nil
}