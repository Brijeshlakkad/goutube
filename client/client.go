package goutube_client

import (
	"bufio"
	"context"
	"fmt"
	"github.com/Brijeshlakkad/goutube"
	streaming_api "github.com/Brijeshlakkad/goutube/api/streaming/v1"
	"github.com/hashicorp/go-hclog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"io"
	"os"
	"path/filepath"
)

// GoutubeClient to fetch the resource using the multiple streams from different goutube servers.
type GoutubeClient struct {
	logger hclog.Logger
	Config
}

type Config struct {
	LocalDir string
	Logger   hclog.Logger
}

func NewGoutubeClient(config Config) *GoutubeClient {
	logger := config.Logger
	if logger == nil {
		logger = hclog.New(&hclog.LoggerOptions{
			Name:   "goutube-client",
			Output: hclog.DefaultOutput,
			Level:  hclog.DefaultLevel,
		})
	}
	return &GoutubeClient{
		Config: config,
	}
}

// Download fetches the object with provided object key from the goutube.
func (g *GoutubeClient) Download(loadbalancerIP string, objectKey string) error {
	ctx := context.Background()
	tlsConfig, err := goutube.SetupTLSConfig(goutube.TLSConfig{
		CertFile: goutube.RootClientCertFile,
		KeyFile:  goutube.RootClientKeyFile,
		CAFile:   goutube.CAFile,
		Server:   false,
	})
	if err != nil {
		return err
	}

	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	conn, err := grpc.Dial(loadbalancerIP, opts...)
	client := streaming_api.NewStreamingClient(conn)
	metadata, err := client.GetMetadata(ctx, &streaming_api.MetadataRequest{
		Point: objectKey,
	})
	if err != nil {
		return err
	}

	var conns []*grpc.ClientConn
	var clients []streaming_api.StreamingClient
	for _, workerIP := range metadata.Workers {
		conn, err := grpc.Dial(workerIP, opts...)
		if err != nil {
			g.logger.Error("couldn't connect to one of the workers")
			continue
		}
		client := streaming_api.NewStreamingClient(conn)
		conns = append(conns, conn)
		clients = append(clients, client)
	}

	workerCount := len(clients)

	var completeChannels = make([]chan struct{}, workerCount)
	var stopCh = make(chan struct{})

	// worker consumes the stream from multiple servers and saves the chunk.
	var worker = func(client streaming_api.StreamingClient, job chunkJob, stopCh chan struct{}) error {
		chunkFile, err := os.OpenFile(job.path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		chunkBuf := bufio.NewWriter(chunkFile)

		defer func() error {
			err := chunkBuf.Flush()
			if err != nil {
				g.logger.Error("err while flushing chunk buffer", err)
			}
			err = chunkFile.Close()
			if err != nil {
				g.logger.Error("err while closing chunk buffer", err)
			}
			return nil
		}()

		clientStream, err := client.ConsumeStream(ctx, &streaming_api.ConsumeRequest{
			Point:  objectKey,
			Offset: job.offset,
			Limit:  job.limit,
		})
		if err != nil {
			return err
		}

	CONSUME:
		for {
			resp, err := clientStream.Recv()
			if err == io.EOF {
				// close the complete channel
				close(job.completeCh)
				// we've reached the end of the stream
				break CONSUME
			}
			_, err = chunkBuf.Write(resp.Frame)
			if err != nil {
				g.logger.Error("err while processing chunk buffer", err)
				return err
			}
			// check if stop signal received
			select {
			case <-stopCh:
				return nil
			default:
			}
		}
		return nil
	}

	size := metadata.Size
	serverTotalChunkSize := int(size) / workerCount

	jobs := make([]chunkJob, workerCount)
	// Assign jobs....
	for i, client := range clients {
		i := i
		client := client
		completeChannels[i] = make(chan struct{}, 1)

		go func() {
			chunkPath := filepath.Join(g.LocalDir, fmt.Sprintf("%s.%d", objectKey, i))
			limit := uint64(serverTotalChunkSize * (i + 1))
			if i == len(clients)-1 {
				limit = 0
			}
			job := chunkJob{
				path:       chunkPath,
				offset:     uint64(serverTotalChunkSize * i),
				limit:      limit,
				completeCh: completeChannels[i],
			}
			jobs[i] = job
			err := worker(client, job, stopCh)
			if err != nil {
				g.logger.Error("worker failed")
			}
		}()
	}

	var waitForChannelsToClose = func(completeChannels []chan struct{}) {
		for _, v := range completeChannels {
			<-v
		}
	}

	waitForChannelsToClose(completeChannels)

	// merge all the chunks to create a new file

	filePath := filepath.Join(g.LocalDir, objectKey)
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	destBuf := bufio.NewWriter(file)
	BUFFERSIZE := 1024

	var chunkFiles []*os.File

	defer func() {
		for _, chunkFile := range chunkFiles {
			err := chunkFile.Close()
			if err != nil {
				g.logger.Error("err while closing chunk buffer", err)
			}
			err = os.Remove(chunkFile.Name())
			if err != nil {
				g.logger.Error("err while removing chunk buffer", err)
			}
		}
		err := destBuf.Flush()
		if err != nil {
			g.logger.Error("err while flushing the mail buffer", err)
		}
		err = file.Close()
		if err != nil {
			g.logger.Error("err while closing chunk buffer", err)
		}
	}()

FILE_WRITER:
	for _, job := range jobs {
		chunkFile, err := os.OpenFile(job.path, os.O_RDWR, 0644)
		if err != nil {
			g.logger.Error("err while opening the chunk buffer", err)
			return err
		}
		chunkFiles = append(chunkFiles, chunkFile)
		buf := make([]byte, BUFFERSIZE)

	CHUNK_READER:
		for {
			n, err := chunkFile.Read(buf)
			if err == io.EOF && n == 0 {
				break CHUNK_READER
			}
			if err != nil {
				g.logger.Error("err while reading the chunk", err)
				return err
			}

			if _, err := destBuf.Write(buf[:n]); err != nil {
				break FILE_WRITER
			}
		}
	}

	return nil
}

type chunkJob struct {
	path       string
	offset     uint64
	limit      uint64
	produceCh  chan []byte
	completeCh chan struct{}
}
