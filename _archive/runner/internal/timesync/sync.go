package timesync

import (
	"sync"
	"time"

	"github.com/shift/runner/internal/logger"
)

// TimeSync manages time synchronization with Hub and peers
type TimeSync struct {
	logger        *logger.Logger
	offset        time.Duration // Offset from system time to Hub time
	offsetMu      sync.RWMutex
	lastSyncTime  time.Time
	lastSyncTimeMu sync.RWMutex
	timezone      string
	timezoneMu    sync.RWMutex
}

// NewTimeSync creates a new time sync manager
func NewTimeSync(log *logger.Logger) *TimeSync {
	return &TimeSync{
		logger:   log,
		offset:   0,
		timezone: "UTC",
	}
}

// SyncWithHub syncs time with Hub's seed time
func (ts *TimeSync) SyncWithHub(serverTimeUnix int64, timezone string) {
	ts.offsetMu.Lock()
	defer ts.offsetMu.Unlock()
	
	ts.timezoneMu.Lock()
	defer ts.timezoneMu.Unlock()
	
	serverTime := time.Unix(serverTimeUnix, 0)
	localTime := time.Now()
	
	// Calculate offset: serverTime - localTime
	ts.offset = serverTime.Sub(localTime)
	ts.timezone = timezone
	ts.lastSyncTimeMu.Lock()
	ts.lastSyncTime = localTime
	ts.lastSyncTimeMu.Unlock()
	
	ts.logger.Info("Synced time with Hub: offset=%v, timezone=%s", ts.offset, timezone)
}

// SyncWithPeer syncs time with a peer runner
func (ts *TimeSync) SyncWithPeer(peerTimeUnix int64, peerOffset time.Duration) {
	ts.offsetMu.Lock()
	defer ts.offsetMu.Unlock()
	
	peerTime := time.Unix(peerTimeUnix, 0)
	localTime := time.Now()
	
	// Calculate offset based on peer's time and offset
	// peerTime + peerOffset = Hub time
	// localTime + ourOffset = Hub time
	// So: ourOffset = (peerTime + peerOffset) - localTime
	hubTime := peerTime.Add(peerOffset)
	ts.offset = hubTime.Sub(localTime)
	
	ts.lastSyncTimeMu.Lock()
	ts.lastSyncTime = localTime
	ts.lastSyncTimeMu.Unlock()
	
	ts.logger.Info("Synced time with peer: offset=%v", ts.offset)
}

// Now returns the current synchronized time
func (ts *TimeSync) Now() time.Time {
	ts.offsetMu.RLock()
	defer ts.offsetMu.RUnlock()
	
	return time.Now().Add(ts.offset)
}

// GetOffset returns the current time offset
func (ts *TimeSync) GetOffset() time.Duration {
	ts.offsetMu.RLock()
	defer ts.offsetMu.RUnlock()
	return ts.offset
}

// GetTimezone returns the configured timezone
func (ts *TimeSync) GetTimezone() string {
	ts.timezoneMu.RLock()
	defer ts.timezoneMu.RUnlock()
	return ts.timezone
}

// GetLastSyncTime returns when the last sync occurred
func (ts *TimeSync) GetLastSyncTime() time.Time {
	ts.lastSyncTimeMu.RLock()
	defer ts.lastSyncTimeMu.RUnlock()
	return ts.lastSyncTime
}


