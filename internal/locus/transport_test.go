package locus

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var DummyData = []byte("request")

func TestTransport_SendCommand(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 1 is consumer
	trans1 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Timeout: 4 * time.Second,
		},
	)
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := CommandRequest{
		Data: DummyData,
	}

	resp := CommandResponse{
		Response: int64(1),
	}

	// Listen for a request
	go func() {
		for {
			select {
			case rpc := <-rpcCh:
				// Verify the command
				req := rpc.Command.(*CommandRequest)
				if !reflect.DeepEqual(req, &args) {
					t.Errorf("command mismatch: %#v %#v", *req, args)
					return
				}
				rpc.Respond(&resp, nil)

			case <-time.After(200 * time.Millisecond):
				return
			}
		}
	}()

	// Transport 2 makes outbound request
	streamLayer2, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 2 is consumer
	trans2 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer2,
			Timeout: 4 * time.Second,
		},
	)
	defer trans2.Close()

	// Create wait group
	wg := &sync.WaitGroup{}
	wg.Add(1)

	appendFunc := func() {
		defer wg.Done()
		var out CommandResponse
		if err := trans2.SendCommand(trans1.LocalAddr(), &args, &out); err != nil {
			t.Fatalf("err: %v", err)
		}

		// Verify the response
		if !reflect.DeepEqual(resp, out) {
			t.Fatalf("command mismatch: %#v %#v", resp, out)
		}
	}

	// Try to do parallel appends, should stress the conn pool
	go appendFunc()

	// Wait for the routines to finish
	wg.Wait()

}

func TestTransport_StartStop(t *testing.T) {
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 1 is consumer
	transport := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Timeout: 4 * time.Second,
		},
	)

	transport.Close()
}

func TestNetworkTransport_SendCommandPipeline(t *testing.T) {
	// Transport 1 is consumer
	streamLayer, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 1 is consumer
	trans1 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer,
			Timeout: 4 * time.Second,
		},
	)
	defer trans1.Close()
	rpcCh := trans1.Consumer()

	// Make the RPC request
	args := CommandRequest{
		Data: DummyData,
	}

	resp := CommandResponse{
		Response: int64(1),
	}

	// Listen for a request
	go func() {
		//for i := 0; i < 10; i++ {
		select {
		case rpc := <-rpcCh:
			// Verify the command
			req := rpc.Command.(*CommandRequest)
			if !reflect.DeepEqual(req, &args) {
				t.Errorf("command mismatch: %#v %#v", *req, args)
				return
			}
			rpc.Respond(&resp, nil)

		case <-time.After(200 * time.Millisecond):
			t.Errorf("timeout")
			return
		}
		//}
	}()

	// Transport 2 makes outbound request
	streamLayer2, err := newTCPStreamLayer("localhost:0", nil)
	require.NoError(t, err)

	// Transport 2 is consumer
	trans2 := NewTransportWithConfig(
		&TransportConfig{
			Stream:  streamLayer2,
			Timeout: 4 * time.Second,
		},
	)
	defer trans2.Close()
	pipeline, err := trans2.PrepareCommandTransport(trans1.LocalAddr())
	require.NoError(t, err)

	//for i := 0; i < 10; i++ {
	pipelineResp := new(CommandResponse)
	_, err = pipeline.SendCommand(&args, pipelineResp)
	require.NoError(t, err)
	//}

	pipelineRespCh := pipeline.Consumer()

	//for i := 0; i < 10; i++ {
	select {
	case ready := <-pipelineRespCh:
		// Verify the response
		if !reflect.DeepEqual(&resp, ready.Response()) {
			t.Fatalf("command mismatch: %#v %#v", &resp, ready.Response())
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timeout")
	}
	//}
	pipeline.Close()
}
