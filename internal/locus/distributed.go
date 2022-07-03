package locus

const RaftRPC = 1

type DistributedLocus struct {
}

func NewDistributedPoint() (*DistributedLocus, error) {
	return &DistributedLocus{}, nil
}

func (o *DistributedLocus) Join(id, addr string) error {
	return nil
}

func (o *DistributedLocus) Leave(id string) error {
	return nil
}
