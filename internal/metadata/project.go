package metadata

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
)

type Project struct {
	ID         string
	datasets   []*Dataset
	datasetMap map[string]*Dataset
	mu         sync.RWMutex
	repo       *Repository
}

func (p *Project) DatasetIDs() []string {
	ids := make([]string, len(p.datasets))
	for i := 0; i < len(p.datasets); i++ {
		ids[i] = p.datasets[i].ID
	}
	return ids
}

// Job reads one job from the store. Jobs are not held on the Project:
// jobs.query records one job per query, and every request loads its
// Project, so carrying them made each request decode every job recorded
// so far (goccy/bigquery-emulator#512).
func (p *Project) Job(ctx context.Context, id string) (*Job, error) {
	return p.repo.FindJob(ctx, p.ID, id)
}

func (p *Project) Jobs(ctx context.Context) ([]*Job, error) {
	return p.repo.FindJobsInProject(ctx, p.ID)
}

func (p *Project) Dataset(id string) *Dataset {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.datasetMap[id]
}

func (p *Project) Datasets() []*Dataset {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.datasets
}

func (p *Project) Insert(ctx context.Context, tx *sql.Tx) error {
	return p.repo.AddProject(ctx, tx, p)
}

func (p *Project) Delete(ctx context.Context, tx *sql.Tx) error {
	return p.repo.DeleteProject(ctx, tx, p)
}

func (p *Project) AddDataset(ctx context.Context, tx *sql.Tx, dataset *Dataset) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, exists := p.datasetMap[dataset.ID]; exists {
		return fmt.Errorf("dataset %s is already created", dataset.ID)
	}
	if err := dataset.Insert(ctx, tx); err != nil {
		return err
	}
	p.datasets = append(p.datasets, dataset)
	p.datasetMap[dataset.ID] = dataset
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func (p *Project) DeleteDataset(ctx context.Context, tx *sql.Tx, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	dataset, exists := p.datasetMap[id]
	if !exists {
		return fmt.Errorf("dataset '%s' is not found in project '%s'", id, p.ID)
	}
	if err := dataset.Delete(ctx, tx); err != nil {
		return err
	}
	newDatasets := make([]*Dataset, 0, len(p.datasets))
	for _, dataset := range p.datasets {
		if dataset.ID == id {
			continue
		}
		newDatasets = append(newDatasets, dataset)
	}
	p.datasets = newDatasets
	delete(p.datasetMap, id)
	if err := p.repo.UpdateProject(ctx, tx, p); err != nil {
		return err
	}
	return nil
}

func (p *Project) AddJob(ctx context.Context, tx *sql.Tx, job *Job) error {
	existing, err := p.repo.findJobs(ctx, tx, p.ID, []string{job.ID})
	if err != nil {
		return err
	}
	if len(existing) != 0 {
		return fmt.Errorf("job %s is already created", job.ID)
	}
	return job.Insert(ctx, tx)
}

func (p *Project) DeleteJob(ctx context.Context, tx *sql.Tx, id string) error {
	jobs, err := p.repo.findJobs(ctx, tx, p.ID, []string{id})
	if err != nil {
		return err
	}
	if len(jobs) == 0 {
		return fmt.Errorf("job '%s' is not found in project '%s'", id, p.ID)
	}
	return jobs[0].Delete(ctx, tx)
}

func NewProject(repo *Repository, id string, datasets []*Dataset) *Project {
	datasetMap := map[string]*Dataset{}
	for _, dataset := range datasets {
		datasetMap[dataset.ID] = dataset
	}
	return &Project{
		ID:         id,
		datasets:   datasets,
		datasetMap: datasetMap,
		repo:       repo,
	}
}
