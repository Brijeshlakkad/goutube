package locus

type Config struct {
	Distributed struct {
		BindAdr     string
		LocalID     string
		Bootstrap   bool
		StreamLayer *StreamLayer
	}
}
