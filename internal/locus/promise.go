package locus

type CommandRequest struct {
	Data []byte
}

type CommandResponse struct {
	Response interface{}
}

type Promise interface {
	Response() interface{}
}

type CommandPromise struct {
	req  *CommandRequest
	resp *CommandResponse

	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func NewCommandPromise(req *CommandRequest) *CommandPromise {
	return &CommandPromise{
		req:        req,
		resp:       &CommandResponse{},
		errCh:      make(chan error, 1),
		ShutdownCh: make(chan struct{}),
	}
}

func (c *CommandPromise) init() {
	c.errCh = make(chan error, 1)
}

func (c *CommandPromise) Request() *CommandRequest {
	return c.req
}

func (c *CommandPromise) Response() interface{} {
	return c.resp
}

func (c *CommandPromise) Error() error {
	if c.err != nil {
		return c.err
	}
	if c.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case c.err = <-c.errCh:
	case <-c.ShutdownCh:
		c.err = ErrArcShutdown
	}
	return c.err
}

func (c *CommandPromise) respond(resp interface{}) *CommandPromise {
	c.resp.Response = resp
	return c
}

func (c *CommandPromise) respondError(err error) *CommandPromise {
	if c.responded {
		return c
	}
	if c.errCh != nil {
		c.errCh <- err
		close(c.errCh)
	}
	c.responded = true
	return c
}

// RPCResponse captures both a response and a potential error.
type RPCResponse struct {
	Response interface{}
	Error    error
}

// RPC has a command, and provides a response mechanism.
type RPC struct {
	Command  interface{}
	RespChan chan<- RPCResponse
}

// Respond is used to respondError with a response, error or both
func (r *RPC) Respond(resp interface{}, err error) {
	r.RespChan <- RPCResponse{resp, err}
}

type ReplicateCommandPromise struct {
	req      *CommandRequest
	expected interface{}
	resp     *CommandResponse
}

func NewReplicateCommandPromise(req *CommandRequest, expected interface{}) *ReplicateCommandPromise {
	return &ReplicateCommandPromise{
		req:      req,
		expected: expected,
	}
}

type shutdownPromise struct {
	arc *Arc
}

func (s *shutdownPromise) Response() interface{} {
	if s.arc == nil {
		return nil
	}
	s.arc.waitShutdown()
	return nil
}
