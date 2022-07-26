package quota

import (
	"context"
	"sync"
	"time"

	api "github.com/tigrisdata/tigris/api/server/v1"
	"github.com/tigrisdata/tigris/server/config"
	"github.com/tigrisdata/tigris/server/metadata"
	"github.com/tigrisdata/tigris/server/transaction"
	"go.uber.org/atomic"
	"golang.org/x/time/rate"
)

var (
	sizeLimitUpdateInterval int64 = 5 // seconds

	ErrRateExceeded        = api.Errorf(api.Code_RESOURCE_EXHAUSTED, "request rate limit exceeded")
	ErrThroughputExceeded  = api.Errorf(api.Code_RESOURCE_EXHAUSTED, "request throughput limit exceeded")
	ErrStorageSizeExceeded = api.Errorf(api.Code_RESOURCE_EXHAUSTED, "data size limit exceeded")
)

type State struct {
	Rate            *rate.Limiter
	WriteThroughput *rate.Limiter
	Size            atomic.Int64
	SizeUpdateAt    atomic.Int64
	SizeLock        sync.Mutex
}

type Manager struct {
	tenantQuota sync.Map
	cfg         *config.QuotaConfig
	tenantMgr   *metadata.TenantManager
	txMgr       *transaction.Manager
}

var mgr Manager

func Init(t *metadata.TenantManager, tx *transaction.Manager, c *config.QuotaConfig) {
	mgr = *newManager(t, tx, c)
}

func Allow(ctx context.Context, namespace string, reqSize int) error {
	if !mgr.cfg.Enabled {
		return nil
	}
	return mgr.check(ctx, namespace, reqSize)
}

func newManager(t *metadata.TenantManager, tx *transaction.Manager, c *config.QuotaConfig) *Manager {
	mgr.cfg = c
	mgr.tenantMgr = t
	mgr.txMgr = tx

	return &Manager{cfg: c, tenantMgr: t, txMgr: tx}
}

func (m *Manager) check(ctx context.Context, namespace string, size int) error {
	is, ok := m.tenantQuota.Load(namespace)
	if !ok {
		// Create new state if didn't exist before
		is = &State{
			Rate:            rate.NewLimiter(rate.Limit(m.cfg.RateLimit), 10),
			WriteThroughput: rate.NewLimiter(rate.Limit(m.cfg.WriteThroughputLimit), m.cfg.WriteThroughputLimit),
		}
		m.tenantQuota.Store(namespace, is)
	}

	s := is.(*State)

	if !s.Rate.Allow() {
		return ErrRateExceeded
	}

	if !s.WriteThroughput.AllowN(time.Now(), size) {
		return ErrThroughputExceeded
	}

	return m.checkStorageSize(ctx, namespace, s, size)
}

func (m *Manager) checkStorageSize(ctx context.Context, namespace string, s *State, size int) error {
	sz := s.Size.Load()

	if time.Now().Unix() < s.SizeUpdateAt.Load()+sizeLimitUpdateInterval {
		if sz+int64(size) >= m.cfg.DataSizeLimit {
			return ErrStorageSizeExceeded
		}
		return nil
	}

	s.SizeLock.Lock()
	defer s.SizeLock.Unlock()

	if time.Now().Unix() >= s.SizeUpdateAt.Load()+sizeLimitUpdateInterval {
		s.SizeUpdateAt.Store(time.Now().Unix())

		t, err := m.tenantMgr.GetTenant(ctx, namespace, m.txMgr)
		if err != nil {
			return err
		}

		dsz, err := t.Size(ctx)
		if err != nil {
			return err
		}

		s.Size.Store(dsz)
	}

	sz = s.Size.Load()

	if sz+int64(size) >= m.cfg.DataSizeLimit {
		return ErrStorageSizeExceeded
	}

	return nil
}
