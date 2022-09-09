package goutube_client

import (
	"context"
	"crypto/tls"
	"fmt"
	"github.com/Brijeshlakkad/goutube"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var (
	testChunkSize          = 256
	testMultiStreamPercent = 50
)

type recordTime struct {
	startTime time.Time
	endTime   time.Time
	logger    hclog.Logger
}

func (r *recordTime) print(id string) {
	r.logger.Info(id, "%s Finished with %f seconds", (r.endTime.Sub(r.startTime)).Seconds())
}

func TestGoutubeClient_Download_With_1_Server(t *testing.T) {
	replicationFactor := 2
	agents, loadBalancerIndices, teardown := setupGoutube(t, 3, replicationFactor, 2)
	defer teardown()

	peerTLSConfig, err := goutube.SetupTLSConfig(goutube.TLSConfig{
		CertFile:      goutube.RootClientCertFile,
		KeyFile:       goutube.RootClientKeyFile,
		CAFile:        goutube.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	loadbalancerAddr, err := agents[loadBalancerIndices[0]].RPCAddr()
	require.NoError(t, err)

	loadBalancerClient := client(t, agents[loadBalancerIndices[0]], peerTLSConfig)

	stream, err := loadBalancerClient.ProduceStream(context.Background())
	require.NoError(t, err)

	expectedOffset := uint64(0)
	expectedPos := uint64(0)
	chunkSize := testChunkSize
	testPointLines := 10
	objectKey := "point-1"
	file := setupFile(t, chunkSize, testPointLines)
	for i := 0; i < testPointLines; i++ {
		expectedOffset = expectedPos
		data := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: objectKey, Frame: data})
		require.NoError(t, err)
		expectedPos = expectedOffset + uint64(len(data))
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, objectKey, resp.Records[0].Point)
	require.Equal(t, expectedOffset, resp.Records[0].Offset)

	time.Sleep(3 * time.Second)

	metadata, err := loadBalancerClient.GetMetadata(context.Background(), &streaming_api.MetadataRequest{Point: objectKey})
	require.NoError(t, err)
	require.Equal(t, replicationFactor*testMultiStreamPercent/100, len(metadata.Workers))

	dataDir, err := ioutil.TempDir("", "agentInstance-test-loci")
	require.NoError(t, err)

	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "goutube-test",
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})

	goutubeClient := NewGoutubeClient(Config{
		LocalDir: dataDir,
		Logger:   logger,
	})

	startTime := time.Now()
	err = goutubeClient.Download(loadbalancerAddr, objectKey)
	require.NoError(t, err)
	endTime := time.Now()

	recordT := &recordTime{
		logger:    logger,
		startTime: startTime,
		endTime:   endTime,
	}

	downloadedFilePath := filepath.Join(dataDir, objectKey)

	// Check if the chunk files were deleted
	files, err := ioutil.ReadDir(dataDir)
	require.Equal(t, 1, len(files))
	require.Equal(t, objectKey, files[0].Name())

	// Check if the file content is correct
	chunkFile, err := os.OpenFile(downloadedFilePath, os.O_RDWR, 0644)
	require.NoError(t, err)
	buf := make([]byte, testChunkSize)

	i := 0
	for {
		n, err := chunkFile.Read(buf)
		if err == io.EOF && n == 0 {
			break
		} else {
			require.NoError(t, err)
		}

		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], buf[:n])
		i++
	}
	require.Equal(t, testPointLines, i)

	defer recordT.print(fmt.Sprintf("Goutube Client with replication factor %d and multi-stream percent %d", replicationFactor, testMultiStreamPercent))
}

func TestGoutubeClient_Download_With_10_Server(t *testing.T) {
	replicationFactor := 4
	agents, loadBalancerIndices, teardown := setupGoutube(t, 3, replicationFactor, 2)
	defer teardown()

	peerTLSConfig, err := goutube.SetupTLSConfig(goutube.TLSConfig{
		CertFile:      goutube.RootClientCertFile,
		KeyFile:       goutube.RootClientKeyFile,
		CAFile:        goutube.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	loadbalancerAddr, err := agents[loadBalancerIndices[0]].RPCAddr()
	require.NoError(t, err)

	loadBalancerClient := client(t, agents[loadBalancerIndices[0]], peerTLSConfig)

	stream, err := loadBalancerClient.ProduceStream(context.Background())
	require.NoError(t, err)

	expectedOffset := uint64(0)
	expectedPos := uint64(0)
	chunkSize := testChunkSize
	testPointLines := 10
	objectKey := "point-1"
	file := setupFile(t, chunkSize, testPointLines)
	for i := 0; i < testPointLines; i++ {
		expectedOffset = expectedPos
		data := file[i*chunkSize : (i+1)*chunkSize]
		err := stream.Send(&streaming_api.ProduceRequest{Point: objectKey, Frame: data})
		require.NoError(t, err)
		expectedPos = expectedOffset + uint64(len(data))
	}
	resp, err := stream.CloseAndRecv()
	require.NoError(t, err)

	require.Equal(t, 1, len(resp.Records))
	require.Equal(t, objectKey, resp.Records[0].Point)
	require.Equal(t, expectedOffset, resp.Records[0].Offset)

	time.Sleep(3 * time.Second)

	metadata, err := loadBalancerClient.GetMetadata(context.Background(), &streaming_api.MetadataRequest{Point: objectKey})
	require.NoError(t, err)
	require.Equal(t, replicationFactor*testMultiStreamPercent/100, len(metadata.Workers))

	dataDir, err := ioutil.TempDir("", "agentInstance-test-loci")
	require.NoError(t, err)

	logger := hclog.New(&hclog.LoggerOptions{
		Name:   "goutube-test",
		Output: hclog.DefaultOutput,
		Level:  hclog.DefaultLevel,
	})

	goutubeClient := NewGoutubeClient(Config{
		LocalDir: dataDir,
		Logger:   logger,
	})

	startTime := time.Now()
	err = goutubeClient.Download(loadbalancerAddr, objectKey)
	require.NoError(t, err)
	endTime := time.Now()

	recordT := &recordTime{
		logger:    logger,
		startTime: startTime,
		endTime:   endTime,
	}
	defer recordT.print(fmt.Sprintf("Goutube Client with replication factor %d and multi-stream percent %d", replicationFactor, testMultiStreamPercent))

	downloadedFilePath := filepath.Join(dataDir, objectKey)

	// Check if the chunk files were deleted
	files, err := ioutil.ReadDir(dataDir)
	require.Equal(t, 1, len(files))
	require.Equal(t, objectKey, files[0].Name())

	// Check if the file content is correct
	chunkFile, err := os.OpenFile(downloadedFilePath, os.O_RDWR, 0644)
	require.NoError(t, err)
	buf := make([]byte, testChunkSize)

	i := 0
	for {
		n, err := chunkFile.Read(buf)
		if err == io.EOF && n == 0 {
			break
		} else {
			require.NoError(t, err)
		}

		require.Equal(t, file[i*chunkSize:(i+1)*chunkSize], buf[:n])
		i++
	}
	require.Equal(t, testPointLines, i)
}

func setupGoutube(t *testing.T, leaderCount int, replicationFactor int, loadBalancerCount int) ([]*goutube.Agent, []int, func()) {
	// setup ring with load balancers
	t.Helper()

	serverTLSConfig, err := goutube.SetupTLSConfig(goutube.TLSConfig{
		CertFile:      goutube.ServerCertFile,
		KeyFile:       goutube.ServerKeyFile,
		CAFile:        goutube.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := goutube.SetupTLSConfig(goutube.TLSConfig{
		CertFile:      goutube.RootClientCertFile,
		KeyFile:       goutube.RootClientKeyFile,
		CAFile:        goutube.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	// each leader has 4 followers.
	var rules []goutube.ParticipationRule
	for i := 0; i < leaderCount; i++ {
		rules = append(rules, goutube.LeaderRule)
		for i := 0; i < replicationFactor; i++ {
			rules = append(rules, goutube.FollowerRule)
		}
	}
	for i := 0; i < loadBalancerCount; i++ {
		rules = append(rules, goutube.LoadBalancerRule)
	}

	var loadBalancerIndices []int

	leaderIndex := 0
	agents := setupTestAgent(t, len(rules), func(i int, agents []*goutube.Agent, config *goutube.AgentConfig) {
		var seedAddresses []string
		var leaderJoinAddrs []string
		rule := rules[i]
		// set seed address
		if i != 0 && rule != goutube.FollowerRule {
			seedAddresses = append(
				seedAddresses,
				agents[0].BindAddr,
			)
		}

		if rule == goutube.FollowerRule {
			replicationAddr, err := agents[leaderIndex].ReplicationRPCAddr()
			require.NoError(t, err)

			leaderJoinAddrs = append(
				leaderJoinAddrs,
				replicationAddr,
			)
		}
		if rule == goutube.LeaderRule {
			leaderIndex = i
		} else if rule == goutube.LoadBalancerRule {
			loadBalancerIndices = append(loadBalancerIndices, i)
		}
		config.SeedAddresses = seedAddresses
		config.LeaderAddresses = leaderJoinAddrs
		config.Rule = rule
		config.ServerTLSConfig = serverTLSConfig
		config.PeerTLSConfig = peerTLSConfig
		config.VirtualNodeCount = 3
		config.MultiStreamPercent = testMultiStreamPercent
	}, func(i int, agents []*goutube.Agent, config *goutube.AgentConfig) {
	})

	return agents, loadBalancerIndices, func() {
		for _, agentInstance := range agents {
			err := agentInstance.Shutdown()
			require.NoError(t, err)
			require.NoError(t,
				os.RemoveAll(agentInstance.DataDir),
			)
		}
	}
}

func setupTestAgent(t *testing.T, count int, callBefore func(i int, agent []*goutube.Agent, config *goutube.AgentConfig), callAfter func(i int, agent []*goutube.Agent, config *goutube.AgentConfig)) []*goutube.Agent {
	var agents []*goutube.Agent
	for i := 0; i < count; i++ {
		ports := dynaport.Get(3)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]
		replicationPort := ports[2]

		dataDir, err := ioutil.TempDir("", "agentInstance-test-loci")
		require.NoError(t, err)

		config := goutube.AgentConfig{
			NodeName:        fmt.Sprintf("%d", i),
			BindAddr:        bindAddr,
			RPCPort:         rpcPort,
			ReplicationPort: replicationPort,
			DataDir:         dataDir,
			ACLModelFile:    goutube.ACLModelFile,
			ACLPolicyFile:   goutube.ACLPolicyFile,
			MaxChunkSize:    uint64(testChunkSize),
		}

		callBefore(i, agents, &config)

		agent, err := goutube.NewAgent(config)
		require.NoError(t, err)

		agents = append(agents, agent)

		callAfter(i, agents, &config)
	}
	return agents
}

func client(
	t *testing.T,
	agent *goutube.Agent,
	tlsConfig *tls.Config,
) streaming_api.StreamingClient {
	t.Helper()

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(rpcAddr, opts...)
	require.NoError(t, err)
	client := streaming_api.NewStreamingClient(conn)
	return client
}

func setupFile(t *testing.T, size int, times int) []byte {
	t.Helper()
	file := make([]byte, size*times)
	for i := 0; i < size*times; i++ {
		file[i] = byte(i)
	}
	return file
}
