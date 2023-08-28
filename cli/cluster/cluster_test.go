package cluster_test

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tarantool/tt/cli/cluster"
)

func TestMakeClusterConfig_global(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"foo"}, "bar")
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	value, err := cconfig.RawConfig.Get([]string{"foo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)
	assert.Len(t, cconfig.Groups, 0)
}

func TestMakeClusterConfig_group(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"groups", "g", "car"}, "bar")
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	value, err := cconfig.RawConfig.Get([]string{"groups", "g", "car"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)

	require.Contains(t, cconfig.Groups, "g")

	value, err = cconfig.Groups["g"].RawConfig.Get([]string{"car"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)
}

func TestMakeClusterConfig_replicaset(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"groups", "g", "replicasets", "r", "zoo"}, "bar")
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	value, err := cconfig.RawConfig.Get(
		[]string{"groups", "g", "replicasets", "r", "zoo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)

	require.Contains(t, cconfig.Groups, "g")
	require.Contains(t, cconfig.Groups["g"].Replicasets, "r")

	value, err = cconfig.Groups["g"].Replicasets["r"].RawConfig.Get([]string{"zoo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)
}

func TestMakeClusterConfig_instance(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{
		"groups", "g", "replicasets", "r", "instances", "i", "foo"}, "bar")
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	value, err := cconfig.RawConfig.Get(
		[]string{"groups", "g", "replicasets", "r", "instances", "i", "foo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)

	require.Contains(t, cconfig.Groups, "g")
	require.Contains(t, cconfig.Groups["g"].Replicasets, "r")
	require.Contains(t, cconfig.Groups["g"].Replicasets["r"].Instances, "i")

	value, err = cconfig.Groups["g"].Replicasets["r"].Instances["i"].
		RawConfig.Get([]string{"foo"})
	assert.NoError(t, err)
	assert.Equal(t, "bar", value)
}

func TestMakeClusterConfig_settings(t *testing.T) {
	config := cluster.NewConfig()
	expected := cluster.ClusterConfig{}
	expected.RawConfig = config
	expected.Groups = nil
	expected.Config.Etcd.Endpoints = []string{"a", "b"}
	expected.Config.Etcd.Username = "user"
	expected.Config.Etcd.Password = "pass"
	expected.Config.Etcd.Prefix = "/prefix"
	expected.Config.Etcd.Ssl.KeyFile = "keyfile"
	expected.Config.Etcd.Ssl.CertFile = "certfile"
	expected.Config.Etcd.Ssl.CaPath = "cafile"
	expected.Config.Etcd.Ssl.CaFile = "capath"
	expected.Config.Etcd.Ssl.VerifyPeer = true
	expected.Config.Etcd.Ssl.VerifyHost = true
	expected.Config.Etcd.Http.Request.Timeout = 123

	config.Set([]string{"config", "etcd", "endpoints"},
		[]any{expected.Config.Etcd.Endpoints[0], expected.Config.Etcd.Endpoints[1]})
	config.Set([]string{"config", "etcd", "username"},
		expected.Config.Etcd.Username)
	config.Set([]string{"config", "etcd", "password"},
		expected.Config.Etcd.Password)
	config.Set([]string{"config", "etcd", "prefix"},
		expected.Config.Etcd.Prefix)
	config.Set([]string{"config", "etcd", "ssl", "ssl_key"},
		expected.Config.Etcd.Ssl.KeyFile)
	config.Set([]string{"config", "etcd", "ssl", "cert_file"},
		expected.Config.Etcd.Ssl.CertFile)
	config.Set([]string{"config", "etcd", "ssl", "ca_path"},
		expected.Config.Etcd.Ssl.CaPath)
	config.Set([]string{"config", "etcd", "ssl", "ca_file"},
		expected.Config.Etcd.Ssl.CaFile)
	config.Set([]string{"config", "etcd", "ssl", "verify_host"},
		expected.Config.Etcd.Ssl.VerifyHost)
	config.Set([]string{"config", "etcd", "ssl", "verify_peer"},
		expected.Config.Etcd.Ssl.VerifyPeer)
	config.Set([]string{"config", "etcd", "http", "request", "timeout"},
		int(expected.Config.Etcd.Http.Request.Timeout))

	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	assert.Equal(t, expected.Config, cconfig.Config)
	assert.Equal(t, expected.Groups, cconfig.Groups)

	expected.RawConfig.ForEach(nil, func(path []string, value any) {
		v, err := cconfig.RawConfig.Get(path)
		assert.NoError(t, err)
		assert.Equal(t, value, v)
	})
}

func TestMakeClusterConfig_empty(t *testing.T) {
	config := cluster.NewConfig()
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	_, err = cconfig.RawConfig.Get(nil)
	assert.Error(t, err)
	assert.Len(t, cconfig.Groups, 0)
}

func TestMakeClusterConfig_error(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"groups"}, true)
	_, err := cluster.MakeClusterConfig(config)
	require.Error(t, err)
}

func TestInstances(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"foo"}, "bar")
	config.Set([]string{"groups", "g", "foo"}, "bar")
	config.Set([]string{"groups", "g", "replicasets", "rr", "foo"}, "bar")
	config.Set([]string{
		"groups", "g", "replicasets", "r", "instances", "a", "foo"}, "bar")
	config.Set([]string{
		"groups", "g", "replicasets", "rr", "instances", "b", "foo"}, "bar")
	config.Set([]string{
		"groups", "g", "foo"}, "bar")
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	assert.ElementsMatch(t, cluster.Instances(cconfig), []string{"a", "b"})
}

func TestInstances_empty(t *testing.T) {
	config := cluster.NewConfig()
	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)

	assert.Len(t, cluster.Instances(cconfig), 0)
}

func TestHasInstance(t *testing.T) {
	name := "foo"
	cases := []struct {
		path     []string
		expected bool
	}{
		{[]string{name}, false},
		{[]string{"app", name}, false},
		{[]string{"groups", name, "bar"}, false},
		{[]string{"groups", name, name}, false},
		{[]string{"groups", name, "replicasets", name, "bar"}, false},
		{[]string{"groups", name, "replicasets", name, "instances", name, "bar"}, true},
	}

	for _, tc := range cases {
		t.Run(strings.Join(tc.path, "_"), func(t *testing.T) {
			config := cluster.NewConfig()
			config.Set(tc.path, true)

			cconfig, err := cluster.MakeClusterConfig(config)
			require.NoError(t, err)
			require.NotNil(t, cconfig.RawConfig)
			assert.Equal(t, tc.expected, cluster.HasInstance(cconfig, name))
		})
	}
}

type expectedInstantiate struct {
	path  []string
	value any
}

func assertExpectedInstantiate(t *testing.T,
	config *cluster.Config, expected []expectedInstantiate) {
	t.Helper()

	expectedPaths := [][]string{}
	for _, e := range expected {
		value, err := config.Get(e.path)
		assert.NoError(t, err)
		assert.Equal(t, e.value, value)
		expectedPaths = append(expectedPaths, e.path)
	}

	foreachPaths := [][]string{}
	config.ForEach(nil, func(path []string, value any) {
		foreachPaths = append(foreachPaths, path)
	})

	assert.ElementsMatch(t, expectedPaths, foreachPaths)
}

func TestInstantiate_global(t *testing.T) {
	config := cluster.NewConfig()
	config.Set([]string{"foo"}, 1)
	config.Set([]string{"bar", "foo"}, 2)
	config.Set([]string{"groups", "a", "replicasets", "b", "instances", "c", "x"}, 3)
	config.Set([]string{"groups", "a", "replicasets", "b", "x"}, 4)
	config.Set([]string{"groups", "a", "x"}, 5)

	expected := []expectedInstantiate{
		{[]string{"foo"}, 1},
		{[]string{"bar", "foo"}, 2},
	}

	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)
	iconfig := cluster.Instantiate(cconfig, "non_exist")
	assertExpectedInstantiate(t, iconfig, expected)
}

func TestInstantiate_inherbit(t *testing.T) {
	config := cluster.NewConfig()
	// Priority.
	config.Set([]string{"groups", "a", "replicasets", "b", "instances", "exist", "foo"}, 1)
	config.Set([]string{"groups", "a", "replicasets", "b", "foo"}, 2)
	config.Set([]string{"groups", "a", "foo"}, 3)
	config.Set([]string{"foo"}, 4)

	config.Set([]string{"groups", "a", "replicasets", "b", "car"}, 2)
	config.Set([]string{"groups", "a", "car"}, 3)
	config.Set([]string{"car"}, 4)

	config.Set([]string{"groups", "a", "voo"}, 3)
	config.Set([]string{"voo"}, 4)

	config.Set([]string{"boo"}, 4)
	config.Set([]string{"bar", "foo"}, 4)

	// Other instances/replicasets/groups.
	config.Set([]string{"groups", "a", "replicasets", "b", "instances", "x", "tar"}, 3)
	config.Set([]string{"groups", "a", "replicasets", "x", "instances", "b", "tar"}, 3)
	config.Set([]string{"groups", "x", "replicasets", "b", "instances", "b", "tar"}, 3)
	config.Set([]string{"groups", "a", "replicasets", "x", "tar"}, 4)
	config.Set([]string{"groups", "x", "tar"}, 4)
	config.Set([]string{"groups", "x", "foo"}, 5)

	expected := []expectedInstantiate{
		{[]string{"foo"}, 1},
		{[]string{"car"}, 2},
		{[]string{"voo"}, 3},
		{[]string{"boo"}, 4},
		{[]string{"bar", "foo"}, 4},
	}

	cconfig, err := cluster.MakeClusterConfig(config)
	require.NoError(t, err)
	require.NotNil(t, cconfig.RawConfig)
	iconfig := cluster.Instantiate(cconfig, "exist")
	assertExpectedInstantiate(t, iconfig, expected)
}

func TestGetClusterConfig_path(t *testing.T) {
	config, err := cluster.GetClusterConfig("testdata/app/config.yaml")

	require.NoError(t, err)
	assert.Equal(t, `app:
  bar: 1
  foo: 1
  hoo: 1
  zoo: 1
groups:
  a:
    bar: 2
    foo: 2
    replicasets:
      b:
        bar: 3
        foo: 3
        instances:
          c:
            foo: 4
    zoo: 2
  b:
    replicasets:
      b:
        instances:
          b:
            too: 3
        too: 3
    too: 2
wal:
  dir: filedir
`, config.RawConfig.String())
	require.Contains(t, config.Groups, "a")
	assert.Equal(t, `bar: 2
foo: 2
replicasets:
  b:
    bar: 3
    foo: 3
    instances:
      c:
        foo: 4
zoo: 2
`, config.Groups["a"].RawConfig.String())
	require.Contains(t, config.Groups["a"].Replicasets, "b")
	assert.Equal(t, `bar: 3
foo: 3
instances:
  c:
    foo: 4
`, config.Groups["a"].Replicasets["b"].RawConfig.String())
	require.Contains(t, config.Groups["a"].Replicasets["b"].Instances, "c")
	assert.Equal(t, `foo: 4
`, config.Groups["a"].Replicasets["b"].Instances["c"].RawConfig.String())
	require.Contains(t, config.Groups, "b")
	assert.Equal(t, `replicasets:
  b:
    instances:
      b:
        too: 3
    too: 3
too: 2
`, config.Groups["b"].RawConfig.String())
	require.Contains(t, config.Groups["b"].Replicasets, "b")
	assert.Equal(t, `instances:
  b:
    too: 3
too: 3
`, config.Groups["b"].Replicasets["b"].RawConfig.String())
	require.Contains(t, config.Groups["b"].Replicasets["b"].Instances, "b")
	assert.Equal(t, `too: 3
`, config.Groups["b"].Replicasets["b"].Instances["b"].RawConfig.String())
}

func TestGetClusterConfig_environment(t *testing.T) {
	os.Setenv("TT_WAL_DIR_DEFAULT", "envdir")
	os.Setenv("TT_WAL_MODE_DEFAULT", "envmode")
	config, err := cluster.GetClusterConfig("testdata/app/config.yaml")

	os.Unsetenv("TT_WAL_DIR_DEFAULT")
	os.Unsetenv("TT_WAL_MODE_DEFAULT")

	require.NoError(t, err)
	assert.Equal(t, `app:
  bar: 1
  foo: 1
  hoo: 1
  zoo: 1
groups:
  a:
    bar: 2
    foo: 2
    replicasets:
      b:
        bar: 3
        foo: 3
        instances:
          c:
            foo: 4
    zoo: 2
  b:
    replicasets:
      b:
        instances:
          b:
            too: 3
        too: 3
    too: 2
wal:
  dir: filedir
  mode: envmode
`, config.RawConfig.String())
}

func TestGetClusterConfig_invalid_apppath(t *testing.T) {
	config, err := cluster.GetClusterConfig("some/non/exist")

	assert.Error(t, err)
	assert.NotNil(t, config)
}

func TestGetClusterConfig_nopath(t *testing.T) {
	config, err := cluster.GetClusterConfig("")
	expected := "a configuration file must be set"

	assert.EqualError(t, err, expected)
	assert.NotNil(t, config)
}

func TestGetInstanceConfig_file(t *testing.T) {
	cconfig, err := cluster.GetClusterConfig("testdata/app/config.yaml")
	require.NoError(t, err)
	config, err := cluster.GetInstanceConfig(cconfig, "c")

	require.NoError(t, err)
	assert.Equal(t, `app:
  bar: 1
  foo: 1
  hoo: 1
  zoo: 1
bar: 3
foo: 4
wal:
  dir: filedir
zoo: 2
`, config.RawConfig.String())
}

func TestGetInstanceConfig_environment(t *testing.T) {
	cconfig, err := cluster.GetClusterConfig("testdata/app/config.yaml")
	require.NoError(t, err)
	os.Setenv("TT_WAL_DIR", "envdir")
	config, err := cluster.GetInstanceConfig(cconfig, "c")
	os.Unsetenv("TT_WAL_DIR")

	require.NoError(t, err)
	require.Equal(t, `app:
  bar: 1
  foo: 1
  hoo: 1
  zoo: 1
bar: 3
foo: 4
wal:
  dir: envdir
zoo: 2
`, config.RawConfig.String())
}

func TestGetInstanceConfig_noinstance(t *testing.T) {
	cconfig, err := cluster.GetClusterConfig("testdata/app/config.yaml")
	require.NoError(t, err)
	_, err = cluster.GetInstanceConfig(cconfig, "unknown")
	expected := "an instance \"unknown\" not found"

	assert.EqualError(t, err, expected)
}