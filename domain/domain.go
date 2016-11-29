// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/perfschema"
	"github.com/pingcap/tidb/sessionctx/variable"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/terror"
)

var ddlLastReloadSchemaTS = "ddl_last_reload_schema_ts"

// Domain represents a storage space. Different domains can use the same database name.
// Multiple domains can be used in parallel without synchronization.
type Domain struct {
	store          kv.Storage
	infoHandle     *infoschema.Handle
	ddl            ddl.DDL
	loadCh         chan time.Duration
	// lastLeaseTS    int64 // nano seconds
	m              sync.Mutex
	SchemaValidity SchemaValidityInfo

	MockReloadFailed MockFailure // It mocks reload failed.
}

type SchemaValidityInfo interface {
	// Update the schema validity info, add an new item, delete the expired item.
	// The schemaVer is valid within leaseGrantTime plus lease duration.
	Update(leaseGrantTime uint64, schemaVer int64)
	// Check is it valid for a transaction to use schemaVer, at timestamp txnTS.
	Check(txnTS uint64, schemaVer int64) bool
	// Latest returns the latest schema version it knows, but not necessary a valid one.
	Latest() int64
}

// loadInfoSchema loads infoschema at startTS into handle, usedSchemaVersion is the currently used
// infoschema version, if it is the same as the schema version at startTS, we don't need to reload again.
// It returns the latest schema version and an error.
func (do *Domain) loadInfoSchema(handle *infoschema.Handle, usedSchemaVersion int64, startTS uint64) (int64, error) {
	snapshot, err := do.store.GetSnapshot(kv.NewVersion(startTS))
	if err != nil {
		return 0, errors.Trace(err)
	}
	m := meta.NewSnapshotMeta(snapshot)
	latestSchemaVersion, err := m.GetSchemaVersion()
	if err != nil {
		return 0, errors.Trace(err)
	}
	if usedSchemaVersion != 0 && usedSchemaVersion == latestSchemaVersion {
		log.Debugf("[ddl] schema version is still %d, no need reload", usedSchemaVersion)
		return latestSchemaVersion, nil
	}
	startTime := time.Now()
	ok, err := do.tryLoadSchemaDiffs(m, usedSchemaVersion, latestSchemaVersion)
	if err != nil {
		// We can fall back to full load, don't need to return the error.
		log.Errorf("[ddl] failed to load schema diff err %v", err)
	}
	if ok {
		log.Infof("[ddl] diff load InfoSchema from version %d to %d, in %v",
			usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
		return latestSchemaVersion, nil
	}

	schemas, err := do.fetchAllSchemasWithTables(m)
	if err != nil {
		return 0, errors.Trace(err)
	}

	newISBuilder, err := infoschema.NewBuilder(handle).InitWithDBInfos(schemas, latestSchemaVersion)
	if err != nil {
		return 0, errors.Trace(err)
	}
	log.Infof("[ddl] full load InfoSchema from version %d to %d, in %v",
		usedSchemaVersion, latestSchemaVersion, time.Since(startTime))
	newISBuilder.Build()
	return latestSchemaVersion, nil
}

func (do *Domain) fetchAllSchemasWithTables(m *meta.Meta) ([]*model.DBInfo, error) {
	allSchemas, err := m.ListDatabases()
	if err != nil {
		return nil, errors.Trace(err)
	}
	splittedSchemas := do.splitForConcurrentFetch(allSchemas)
	doneCh := make(chan error, len(splittedSchemas))
	for _, schemas := range splittedSchemas {
		go do.fetchSchemasWithTables(schemas, m, doneCh)
	}
	for range splittedSchemas {
		err = <-doneCh
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	return allSchemas, nil
}

const fetchSchemaConcurrency = 8

func (do *Domain) splitForConcurrentFetch(schemas []*model.DBInfo) [][]*model.DBInfo {
	groupSize := (len(schemas) + fetchSchemaConcurrency - 1) / fetchSchemaConcurrency
	splitted := make([][]*model.DBInfo, 0, fetchSchemaConcurrency)
	schemaCnt := len(schemas)
	for i := 0; i < schemaCnt; i += groupSize {
		end := i + groupSize
		if end > schemaCnt {
			end = schemaCnt
		}
		splitted = append(splitted, schemas[i:end])
	}
	return splitted
}

func (do *Domain) fetchSchemasWithTables(schemas []*model.DBInfo, m *meta.Meta, done chan error) {
	for _, di := range schemas {
		if di.State != model.StatePublic {
			// schema is not public, can't be used outside.
			continue
		}
		tables, err := m.ListTables(di.ID)
		if err != nil {
			done <- err
			return
		}
		di.Tables = make([]*model.TableInfo, 0, len(tables))
		for _, tbl := range tables {
			if tbl.State != model.StatePublic {
				// schema is not public, can't be used outside.
				continue
			}
			di.Tables = append(di.Tables, tbl)
		}
	}
	done <- nil
}

const (
	initialVersion         = 0
	maxNumberOfDiffsToLoad = 100
)

// tryLoadSchemaDiffs tries to only load latest schema changes.
// Returns true if the schema is loaded successfully.
// Returns false if the schema can not be loaded by schema diff, then we need to do full load.
func (do *Domain) tryLoadSchemaDiffs(m *meta.Meta, usedVersion, newVersion int64) (bool, error) {
	if usedVersion == initialVersion || newVersion-usedVersion > maxNumberOfDiffsToLoad {
		// If there isn't any used version, or used version is too old, we do full load.
		return false, nil
	}
	if usedVersion > newVersion {
		// When user use History Read feature, history schema will be loaded.
		// usedVersion may be larger than newVersion, full load is needed.
		return false, nil
	}
	var diffs []*model.SchemaDiff
	for usedVersion < newVersion {
		usedVersion++
		diff, err := m.GetSchemaDiff(usedVersion)
		if err != nil {
			return false, errors.Trace(err)
		}
		if diff == nil {
			// If diff is missing for any version between used and new version, we fall back to full reload.
			return false, nil
		}
		diffs = append(diffs, diff)
	}
	builder := infoschema.NewBuilder(do.infoHandle).InitWithOldInfoSchema()
	for _, diff := range diffs {
		err := builder.ApplyDiff(m, diff)
		if err != nil {
			return false, errors.Trace(err)
		}
	}
	builder.Build()
	return true, nil
}

// InfoSchema gets information schema from domain.
func (do *Domain) InfoSchema() infoschema.InfoSchema {
	return do.infoHandle.Get()
}

// GetSnapshotInfoSchema gets a snapshot information schema.
func (do *Domain) GetSnapshotInfoSchema(snapshotTS uint64) (infoschema.InfoSchema, error) {
	snapHandle := do.infoHandle.EmptyClone()
	_, err := do.loadInfoSchema(snapHandle, do.infoHandle.Get().SchemaMetaVersion(), snapshotTS)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return snapHandle.Get(), nil
}

// PerfSchema gets performance schema from domain.
func (do *Domain) PerfSchema() perfschema.PerfSchema {
	return do.infoHandle.GetPerfHandle()
}

// DDL gets DDL from domain.
func (do *Domain) DDL() ddl.DDL {
	return do.ddl
}

// Store gets KV store from domain.
func (do *Domain) Store() kv.Storage {
	return do.store
}

// SetLease will reset the lease time for online DDL change.
func (do *Domain) SetLease(lease time.Duration) {
	if lease <= 0 {
		log.Warnf("[ddl] set the current lease:%v into a new lease:%v failed, so do nothing",
			do.ddl.GetLease(), lease)
		return
	}

	do.loadCh <- lease
	// let ddl to reset lease too.
	do.ddl.SetLease(lease)
	// TODO: we should remove SetLease totally!!
	do.SchemaValidity.(*schemaValidityInfo).setLease(lease)
}

// // Stats returns the domain statistic.
// func (do *Domain) Stats() (map[string]interface{}, error) {
// 	m := make(map[string]interface{})
// 	m[ddlLastReloadSchemaTS] = atomic.LoadInt64(&do.lastLeaseTS) / 1e9

// 	return m, nil
// }

// GetScope gets the status variables scope.
func (do *Domain) GetScope(status string) variable.ScopeFlag {
	// Now domain status variables scope are all default scope.
	return variable.DefaultScopeFlag
}

func (do *Domain) mockReloadFailed() error {
	// ver, err := do.store.CurrentVersion()
	// if err != nil {
	// 	log.Errorf("mock reload failed err:%v", err)
	// 	return errors.Trace(err)
	// }
	// lease := do.DDL().GetLease()
	// // Make sure that is timed out when checking validity.
	// mockLastSuccTime := time.Now().UnixNano() - int64(lease)
	// log.Warnf("mock lastSuccTS:%v, lease:%v", time.Now(), time.Duration(lease))
	// do.SchemaValidity.updateTimeInfo(mockLastSuccTime, ver.Ver)
	log.Warnf("mock reload fail.")
	return errors.New("mock reload failed")
}

// Reload reloads InfoSchema.
// It's public in order to do the test.
func (do *Domain) Reload() error {
	// for test
	if do.MockReloadFailed.getValue() {
		return do.mockReloadFailed()
	}

	// Lock here for only once at the same time.
	do.m.Lock()
	defer do.m.Unlock()

	startTime := time.Now()

	var err error
	var latestSchemaVersion int64

	ver, err := do.store.CurrentVersion()
	if err != nil {
		return errors.Trace(err)
	}

	schemaVersion := int64(0)
	oldInfoSchema := do.infoHandle.Get()
	if oldInfoSchema != nil {
		schemaVersion = oldInfoSchema.SchemaMetaVersion()
	}

	latestSchemaVersion, err = do.loadInfoSchema(do.infoHandle, schemaVersion, ver.Ver)
	if err != nil {
		return errors.Trace(err)
	}

	log.Infof("reload schema success, update %d, %d", ver.Ver, latestSchemaVersion)
	do.SchemaValidity.Update(ver.Ver, latestSchemaVersion)

	lease := do.DDL().GetLease()
	sub := time.Since(startTime)
	if sub > lease && lease > 0 {
		log.Warnf("[ddl] loading schema takes a long time %v", sub)
	}

	return nil
}

// minInterval gets a minimal interval.
// It uses to reload schema and check schema validity after the schema is invalid.
// If lease is 0, it's used for local store and minimal interval is 5ms.
func minInterval(lease time.Duration) time.Duration {
	if lease > 0 {
		return lease / 2
	}
	return 5 * time.Millisecond
}

func (do *Domain) loadSchemaInLoop(lease time.Duration) {
	// lease renewal process can run at any frequency, not bind to the lease value.
	// set it to lease/2 here, as recommend by the paper.
	timer := time.NewTimer(minInterval(lease))

	for {
		select {
		case <-timer.C:
			err := do.Reload()
			if err != nil {
				log.Errorf("[ddl] reload schema in loop err %v", errors.ErrorStack(err))
			}
		case v:=<-do.loadCh:
			lease = v
			if !timer.Stop() {
				<-timer.C
			}
		}
		if lease != 0 {
			timer.Reset(minInterval(lease))
		}
	}
}

type ddlCallback struct {
	ddl.BaseCallback
	do *Domain
}

func (c *ddlCallback) OnChanged(err error) error {
	if err != nil {
		return err
	}
	log.Infof("[ddl] on DDL change, must reload")

	err = c.do.Reload()
	if err != nil {
		log.Errorf("[ddl] on DDL change reload err %v", err)
	}

	return nil
}

// MockFailure mocks reload failed.
// It's used for fixing data race in tests.
type MockFailure struct {
	sync.RWMutex
	val bool // val is true means we need to mock reload failed.
}

// SetValue sets whether we need to mock reload failed.
func (m *MockFailure) SetValue(isFailed bool) {
	m.Lock()
	defer m.Unlock()
	m.val = isFailed
}

func (m *MockFailure) getValue() bool {
	m.RLock()
	defer m.RUnlock()
	return m.val
}

type schemaValidityInfo struct {
	mux   sync.RWMutex
	lease time.Duration
	items map[int64]time.Time
	latestSchemaVer int64
}

func newSchemaValidityInfo(lease time.Duration) SchemaValidityInfo {
	return &schemaValidityInfo{
		lease: lease,
		items: make(map[int64]time.Time),
	}
}

func extractPhysicalTime(ts uint64) time.Time {
	t := oracle.ExtractPhysical(ts)
	return time.Unix(t/1e3, (t%1e3)*1e6)
}

func (s *schemaValidityInfo) Update(leaseGrantTS uint64, schemaVer int64) {
	s.mux.Lock()

	s.latestSchemaVer = schemaVer
	leaseGrantTime := extractPhysicalTime(leaseGrantTS)
	leaseExpire := leaseGrantTime.Add(s.lease)

	// renewal lease
	s.items[schemaVer] = leaseExpire
	log.Infof("update schema %d expire at %v", schemaVer, leaseExpire)

	// delete expired items, leastGrantTime is server current time, actually.
	for k, expire := range s.items {
		if leaseGrantTime.After(expire) {
			log.Infof("recycle expired lease %d...version %d %v %v", s.lease, k, expire, leaseGrantTime)
			delete(s.items, k)
		}
	}

	s.mux.Unlock()
}

// Check checks schema validity. It returns the current schema version and an error.
func (s *schemaValidityInfo) Check(txnTS uint64, schemaVer int64) bool {
	s.mux.RLock()
	defer s.mux.RUnlock()

	if s.lease == 0 {
		return true
	}

	expire, ok := s.items[schemaVer]
	if !ok {
		// Can't find schema version means it's already expired.
		log.Info("cant find schema .................................")
		return false
	}

	t := extractPhysicalTime(txnTS)
	if t.After(expire) {
		log.Infof("lease expired time %v, txnTS is %v", expire, t)
		return false
	}

	return true
}

// Latest returns the latest schema version it knows.
func (s *schemaValidityInfo) Latest() int64 {
	return s.latestSchemaVer
}

func (s *schemaValidityInfo) setLease(lease time.Duration) {
	for k, v := range s.items {
		s.items[k] = v.Add(lease)
	}
	s.lease += lease
}

// NewDomain creates a new domain. Should not create multiple domains for the same store.
func NewDomain(store kv.Storage, lease time.Duration) (d *Domain, err error) {
	d = &Domain{
		store:          store,
		SchemaValidity: newSchemaValidityInfo(lease),
		loadCh: make(chan time.Duration, 1),
	}

	d.infoHandle, err = infoschema.NewHandle(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	d.ddl = ddl.NewDDL(d.store, d.infoHandle, &ddlCallback{do: d}, lease)
	if err = d.Reload(); err != nil {
		return nil, errors.Trace(err)
	}

	// variable.RegisterStatistics(d)

	// Local store needs to get the change information for every DDL state in each session.
	go d.loadSchemaInLoop(lease)

	return d, nil
}

// Domain error codes.
const (
	codeInfoSchemaExpired terror.ErrCode = 1
	codeInfoSchemaChanged terror.ErrCode = 2
)

var (
	// ErrInfoSchemaExpired returns the error that information schema is out of date.
	ErrInfoSchemaExpired = terror.ClassDomain.New(codeInfoSchemaExpired, "Infomation schema is out of date.")
	// ErrInfoSchemaChanged returns the error that information schema is changed.
	ErrInfoSchemaChanged = terror.ClassDomain.New(codeInfoSchemaChanged, "Infomation schema is changed.")
)
