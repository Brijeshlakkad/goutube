package server

import (
	replication_api "github.com/Brijeshlakkad/goutube/api/replication/v1"
)

type ReplicationManager struct {
	replication_api.UnimplementedReplicationServer
	*ReplicationConfig
}

type ReplicationConfig struct {
	LociManager LociManager
}

func NewReplicationManager(config *ReplicationConfig) (*ReplicationManager, error) {
	return &ReplicationManager{
		ReplicationConfig: config,
	}, nil
}

func (l *ReplicationManager) Replicate(req *replication_api.ReplicateRequest, stream replication_api.Replication_ReplicateServer) error {
	lenWidth := 8
	defer (func() error {
		if err := l.LociManager.CloseAll(); err != nil {
			return err
		}
		return nil
	})()
	var locusId string
	var pointId string
	for _, locusId = range l.LociManager.GetLoci() {
		for _, pointId = range l.LociManager.GetPoints(locusId) {
			var end = false
			off := int64(0)
			for !end {
				select {
				case <-stream.Context().Done():
					return nil
				default:
					buf := make([]byte, lenWidth)
					n, err := l.LociManager.ReadAt(locusId, pointId, buf, off)
					if err != nil {
						end = true
					}
					off += int64(n)

					size := enc.Uint64(buf)
					buf = make([]byte, size)
					n, err = l.LociManager.ReadAt(locusId, pointId, buf, off)
					if err != nil {
						return err
					}
					off += int64(n)
					if err := stream.Send(&replication_api.ReplicateResponse{
						Locus: locusId,
						Point: pointId,
						Frame: buf,
					}); err != nil {
						return err
					}
				}
			}

			if err := l.LociManager.ClosePoint(locusId, pointId); err != nil {
				return err
			}
		}
	}
	return nil
}
