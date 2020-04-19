package todos

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/fatih/structs"
	"github.com/google/uuid"
)

type token struct {
	createdTime time.Time
	token       *gocb.MutationToken
}

type MutationTokenStorage struct {
	sync.RWMutex
	clearTicker *time.Ticker
	stopCh      chan chan struct{}
	storage     map[string][]*token
}

func NewMutationTokenStorage() *MutationTokenStorage {
	s := &MutationTokenStorage{
		clearTicker: time.NewTicker(time.Second * 60),
		storage:     make(map[string][]*token),
	}

	go func() {
		for {
			select {
			case <-s.clearTicker.C:
				s.ClearExpired()
			case ch := <-s.stopCh:
				s.clearTicker.Stop()
				close(ch)
			}
		}
	}()

	return s
}

func (s *MutationTokenStorage) Add(entityType string, t *gocb.MutationToken) {
	s.Lock()
	defer s.Unlock()
	s.storage[entityType] = append(s.storage[entityType], &token{
		token:       t,
		createdTime: time.Now(),
	})
}

func (s *MutationTokenStorage) Get(entityType string) []*token {
	s.RLock()
	defer s.RUnlock()
	return s.storage[entityType]
}

func (s *MutationTokenStorage) ClearExpired() {
	s.Lock()
	defer s.Unlock()
	for k, v := range s.storage {
		var tokens []*token
		for _, tk := range v {
			if !time.Now().After(tk.createdTime.Add(time.Hour)) {
				tokens = append(tokens, tk)
			}
		}
		s.storage[k] = tokens
	}
}

func (s *MutationTokenStorage) Close() {
	ch := make(chan struct{})
	s.stopCh <- ch
}

type collection struct {
	entityType   string
	c            *gocb.Collection
	tokenStorage *MutationTokenStorage
}

func (c *collection) Get(id string, opts *gocb.GetOptions) (*gocb.GetResult, error) {
	return c.c.Get(id, opts)
}

func (c *collection) Insert(id string, value interface{}, opts *gocb.InsertOptions) (*gocb.MutationResult, error) {
	res, err := c.c.Insert(id, value, opts)
	if c.tokenStorage != nil && err == nil {
		c.tokenStorage.Add(c.entityType, res.MutationToken())
	}
	return res, err
}

func (c *collection) Replace(id string, value interface{}, opts *gocb.ReplaceOptions) (*gocb.MutationResult, error) {
	res, err := c.c.Replace(id, value, opts)
	if c.tokenStorage != nil && err == nil {
		c.tokenStorage.Add(c.entityType, res.MutationToken())
	}
	return res, err
}

func (c *collection) Remove(id string, opts *gocb.RemoveOptions) (*gocb.MutationResult, error) {
	res, err := c.c.Remove(id, opts)
	if c.tokenStorage != nil && err == nil {
		c.tokenStorage.Add(c.entityType, res.MutationToken())
	}
	return res, err
}

type Domain struct {
	cluster      *gocb.Cluster
	bucket       *gocb.Bucket
	tokenStorage *MutationTokenStorage
}

type QueryOptions struct {
	AtPlus          bool
	ScanConsistency gocb.QueryScanConsistency
	Collections     []string
}

func (d *Domain) Query(query string, opts *QueryOptions, params ...interface{}) (*gocb.QueryResult, error) {
	var queryOpts gocb.QueryOptions
	if opts.AtPlus {
		var tks []gocb.MutationToken
		for _, collection := range opts.Collections {
			for _, t := range d.tokenStorage.Get(collection) {
				tks = append(tks, *t.token)
			}
		}
		queryOpts.ConsistentWith = gocb.NewMutationState(tks...)
	}
	for _, p := range params {
		queryOpts.PositionalParameters = append(queryOpts.PositionalParameters, p)
	}
	if opts.ScanConsistency > 0 {
		queryOpts.ScanConsistency = opts.ScanConsistency
	}
	return d.cluster.Query(query, &queryOpts)
}

func (d *Domain) col(entityType string) *collection {
	if entityType == "" {
		entityType = d.bucket.Name()
	}
	return &collection{
		entityType:   entityType,
		tokenStorage: d.tokenStorage,
		c:            d.bucket.DefaultCollection(),
	}
}

func (d *Domain) Get(id string, data interface{}) (gocb.Cas, error) {
	res, err := d.col("").Get(id, &gocb.GetOptions{})
	if err != nil {
		return 0, err
	}
	if err := res.Content(data); err != nil {
		return 0, err
	}
	return res.Cas(), nil
}

func (d *Domain) Create(data interface{}) (string, error) {
	id := uuid.New().String()
	s := structs.New(data)
	if f, ok := s.FieldOk("ID"); ok {
		f.Set(id)
	}
	_, err := d.col("").Insert(id, data, &gocb.InsertOptions{})
	return id, err
}

func (d *Domain) Update(id string, cas gocb.Cas, data interface{}) (gocb.Cas, error) {
	s := structs.New(data)
	if f, ok := s.FieldOk("ID"); ok {
		f.Set(id)
	}
	mr, err := d.col("").Replace(id, data, &gocb.ReplaceOptions{
		Cas: cas,
	})
	if err != nil {
		return 0, nil
	}
	return mr.Cas(), err
}

func (d *Domain) Delete(id string, cas gocb.Cas) (gocb.Cas, error) {
	mr, err := d.col("").Remove(id, &gocb.RemoveOptions{
		Cas: cas,
	})
	if err != nil {
		return 0, nil
	}
	return mr.Cas(), err
}

type DB struct {
	sync.RWMutex
	tokenStorage *MutationTokenStorage
	domains      map[string]*Domain
}

type AddDomainOptions struct {
	ConnStr  string
	Name     string
	Username string
	Password string
}

func init() {

	if err := AddDomain(&AddDomainOptions{
		ConnStr:  "couchbase://127.0.0.1",
		Name:     "system",
		Username: "Administrator",
		Password: "password",
	}); err != nil {
		log.Fatal("add domain error", err)
	}

	domainMgr := std.domains["system"]
	cluster := domainMgr.cluster

	bucket := cluster.Bucket("system")

	_, err := bucket.Ping(&gocb.PingOptions{})
	if err != nil {
		log.Fatal("ping response error", err)
	}

	fmt.Println("open bucket system")

}

var std *DB = &DB{
	tokenStorage: NewMutationTokenStorage(),
	domains:      make(map[string]*Domain),
}

func (db *DB) GetDomain(name string) (*Domain, error) {
	db.RLock()
	defer db.RUnlock()
	if domain, ok := db.domains[name]; ok {
		return domain, nil
	}
	return nil, errors.New("domain not found")
}

func (db *DB) AddDomain(opts *AddDomainOptions) error {

	db.Lock()
	defer db.Unlock()

	if _, ok := db.domains[opts.Name]; ok {
		return errors.New("domain already registered")
	}

	var err error
	var cluster *gocb.Cluster

	for i := 0; i < 5; i++ {
		cluster, err = gocb.Connect(opts.ConnStr, gocb.ClusterOptions{
			Username: opts.Username,
			Password: opts.Password,
			TimeoutsConfig: gocb.TimeoutsConfig{
				ConnectTimeout: time.Second * 60,
				KVTimeout:      time.Second * 60,
				QueryTimeout:   time.Second * 1800,
			},
		})
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}

	if err != nil {
		return fmt.Errorf("connect to cluster error: %w", err)
	}

	bucket := cluster.Bucket(opts.Name)

	_, err = bucket.Ping(&gocb.PingOptions{})
	if err != nil {
		return err
	}

	db.domains[opts.Name] = &Domain{
		cluster:      cluster,
		bucket:       bucket,
		tokenStorage: NewMutationTokenStorage(),
	}

	return nil
}

func (db *DB) CreatePrimaryIndex(domainName string) error {

	systemMgr, ok := db.domains["system"]
	if !ok {
		return errors.New("system domain not registered")
	}

	queryIndexes, err := systemMgr.cluster.QueryIndexes().GetAllIndexes(domainName, &gocb.GetAllQueryIndexesOptions{})
	if err != nil {
		return err
	}

	for _, i := range queryIndexes {
		if i.Name == "#primary" {
			return nil
		}
	}

	return systemMgr.cluster.QueryIndexes().CreatePrimaryIndex(
		domainName, &gocb.CreatePrimaryQueryIndexOptions{})
}

func (db *DB) CreateMainBucketIfNotExists(domainName string) error {

	systemMgr, ok := db.domains["system"]
	if !ok {
		return errors.New("system domain not registered")
	}

	bucketManager := systemMgr.cluster.Buckets()

	_, err := bucketManager.GetBucket(domainName, nil)
	if err != nil && strings.Contains(err.Error(), "bucket not found") {
		bucketSettings := gocb.CreateBucketSettings{
			BucketSettings: gocb.BucketSettings{
				Name:       domainName,
				RAMQuotaMB: 500,
				BucketType: gocb.CouchbaseBucketType,
			},
		}
		if err := bucketManager.CreateBucket(bucketSettings, nil); err != nil {
			return err
		}
		return nil
	}
	return err
}

func (db *DB) CreateUserIfNotExists(domainName, username, password string) error {

	systemMgr, ok := db.domains["system"]
	if !ok {
		return errors.New("system domain not registered")
	}

	cluster := systemMgr.cluster
	users := cluster.Users()
	existedUser, err := users.GetUser(username, &gocb.GetUserOptions{})

	var roles []gocb.Role

	if err == nil {
		roles = existedUser.Roles
	} else if err != nil && !strings.Contains(err.Error(), "user not found") {
		return err
	}

	extendRole := true

	for _, r := range roles {
		if r.Bucket == domainName {
			extendRole = false
			break
		}
	}

	if extendRole {
		roles = append(roles, gocb.Role{
			Name:   "bucket_full_access",
			Bucket: domainName,
		})
	}

	return users.UpsertUser(gocb.User{Username: username, Password: password, Roles: roles}, &gocb.UpsertUserOptions{})
}

func GetDomain(name string) (*Domain, error) {
	return std.GetDomain(name)
}

func AddDomain(opts *AddDomainOptions) error {
	return std.AddDomain(opts)
}

func CreatePrimaryIndex(domainName string) error {
	return std.CreatePrimaryIndex(domainName)
}

func CreateUserIfNotExists(domainName, username, password string) error {
	return std.CreateUserIfNotExists(domainName, username, password)
}

func CreateMainBucketIfNotExists(domainName string) error {
	return std.CreateMainBucketIfNotExists(domainName)
}
