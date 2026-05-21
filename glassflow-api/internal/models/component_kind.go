package models

// ComponentKind identifies which pipeline component produced a DLQ message or component signal.
type ComponentKind string

const (
	ComponentKindSink        ComponentKind = "sink"
	ComponentKindIngestor    ComponentKind = "ingestor"
	ComponentKindJoin        ComponentKind = "join"
	ComponentKindDedup       ComponentKind = "dedup"
	ComponentKindBatchWriter ComponentKind = "batch-writer"
)

func (k ComponentKind) String() string {
	return string(k)
}
