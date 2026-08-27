package workitems

import "context"

// Service is the boundary consumed by core.Hub. WebSocket routing depends on
// this API, never on SQLite details, so storage migrations and synchronization
// policy remain local to this package.
type Service struct {
	store *Store
}

func OpenService(dataDir, instanceID string) (*Service, error) {
	store, err := Open(dataDir, instanceID)
	if err != nil {
		return nil, err
	}
	return &Service{store: store}, nil
}

func (s *Service) Close() error { return s.store.Close() }

func (s *Service) CreateProject(ctx context.Context, in CreateProjectInput) (Project, error) {
	return s.store.CreateProject(ctx, in)
}

func (s *Service) CreateItem(ctx context.Context, in CreateItemInput) (WorkItem, error) {
	return s.store.CreateItem(ctx, in)
}

func (s *Service) UpdateItem(ctx context.Context, in UpdateItemInput) (WorkItem, error) {
	return s.store.UpdateItem(ctx, in)
}

func (s *Service) MoveItem(ctx context.Context, in MoveItemInput) (WorkItem, error) {
	return s.store.MoveItem(ctx, in)
}

func (s *Service) LinkSession(ctx context.Context, in LinkSessionInput) (SessionLink, WorkItem, error) {
	return s.store.LinkSession(ctx, in)
}

func (s *Service) AddDependency(ctx context.Context, in AddDependencyInput) (WorkItem, error) {
	return s.store.AddDependency(ctx, in)
}

func (s *Service) GetItem(ctx context.Context, id string) (WorkItem, error) {
	return s.store.GetItem(ctx, id)
}

func (s *Service) Snapshot(ctx context.Context) (Snapshot, error) {
	return s.store.Snapshot(ctx)
}
