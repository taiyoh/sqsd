package sqsd

func (t *QueueTracker) Find(id string) (Queue, bool) {
	data, exists := t.currentWorkings.Load(id)
	if !exists {
		return Queue{}, exists
	}
	return data.(Queue), exists
}
