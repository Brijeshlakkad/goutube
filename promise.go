package goutube

type RecordRequest struct {
	Data []byte
}

type RecordResponse struct {
	LastOff  uint64
	Response interface{}
}

type FSMRecordResponse struct {
	StoreKey   interface{}
	StoreValue interface{}
	Response   interface{}
}

type Promise interface {
	Error() error
	Response() interface{}
}

type promiseError struct {
	err        error
	errCh      chan error
	responded  bool
	ShutdownCh chan struct{}
}

func (p *promiseError) init() {
	p.errCh = make(chan error, 1)
}

func (p *promiseError) Error() error {
	if p.err != nil {
		return p.err
	}
	if p.errCh == nil {
		panic("waiting for response on nil channel")
	}
	select {
	case p.err = <-p.errCh:
	case <-p.ShutdownCh:
		p.err = ErrArcShutdown
	}
	return p.err
}

func (p *promiseError) respondError(err error) {
	if p.responded {
		return
	}
	if p.errCh != nil {
		p.errCh <- err
		close(p.errCh)
	}
	p.responded = true
}

type RecordPromise struct {
	promiseError
	req  *RecordRequest
	resp *RecordResponse
}

func (c *RecordPromise) init() {
	c.promiseError.init()
}

func (c *RecordPromise) Request() *RecordRequest {
	return c.req
}

func (c *RecordPromise) Response() interface{} {
	return c.resp
}

func (c *RecordPromise) respond(resp interface{}) *RecordPromise {
	c.resp.Response = resp
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
	req      *RecordRequest
	expected interface{}
	resp     *RecordResponse
}

func NewReplicateCommandPromise(req *RecordRequest, expected interface{}) *ReplicateCommandPromise {
	return &ReplicateCommandPromise{
		req:      req,
		expected: expected,
	}
}

type shutdownPromise struct {
	arc *Arc
}

func (s *shutdownPromise) Error() error {
	if s.arc == nil {
		return nil
	}
	s.arc.waitShutdown()
	return nil
}

func (s *shutdownPromise) Response() interface{} {
	return nil
}

type RecordEntriesRequest struct {
	Entries []*RecordRequest
}

type RecordEntriesResponse struct {
	LastOff  uint64
	Response interface{}
}

type RecordEntriesPromise struct {
	promiseError
	req  *RecordEntriesRequest
	resp *RecordEntriesResponse
}

func (c *RecordEntriesPromise) init() {
	c.promiseError.init()
}

func (c *RecordEntriesPromise) Request() *RecordEntriesRequest {
	return c.req
}

func (c *RecordEntriesPromise) Response() interface{} {
	return c.resp
}

func (c *RecordEntriesPromise) respond(resp interface{}) *RecordEntriesPromise {
	c.resp.Response = resp
	return c
}
