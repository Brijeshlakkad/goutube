package mandala

type Server struct {
	BindAddr string
	Peers    map[string]string
}

func NewServer(bindArr string) (*Server, error) {
	return &Server{
		BindAddr: bindArr,
		Peers:    make(map[string]string),
	}, nil
}

func (s *Server) AddPeer(id string, bindAddr string) error {
	if _, ok := s.Peers[id]; ok {
		return nil
	}
	s.Peers[id] = bindAddr
	return nil
}

func (s *Server) RemovePeer(id string) error {
	if _, ok := s.Peers[id]; ok {
		return nil
	}
	delete(s.Peers, id)
	return nil
}
