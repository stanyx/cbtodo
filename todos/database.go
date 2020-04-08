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

type Domain struct {
	cluster *gocb.Cluster
	bucket  *gocb.Bucket
}

func (d *Domain) Query(query string) (*gocb.QueryResult, error) {
	return d.cluster.Query(query, &gocb.QueryOptions{})
}

func (d *Domain) Get(id string, data interface{}) (gocb.Cas, error) {
	res, err := d.bucket.DefaultCollection().Get(id, &gocb.GetOptions{})
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
	_, err := d.bucket.DefaultCollection().Insert(id, data, &gocb.InsertOptions{})
	return id, err
}

func (d *Domain) Update(id string, cas gocb.Cas, data interface{}) (gocb.Cas, error) {
	s := structs.New(data)
	if f, ok := s.FieldOk("ID"); ok {
		f.Set(id)
	}
	mr, err := d.bucket.DefaultCollection().Replace(id, data, &gocb.ReplaceOptions{
		Cas: cas,
	})

	if err != nil {
		return 0, nil
	}
	return mr.Cas(), err
}

func (d *Domain) Delete(id string, cas gocb.Cas) (gocb.Cas, error) {
	mr, err := d.bucket.DefaultCollection().Remove(id, &gocb.RemoveOptions{
		Cas: cas,
	})
	if err != nil {
		return 0, nil
	}
	return mr.Cas(), err
}

type DB struct {
	sync.RWMutex
	domains map[string]*Domain
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

var std *DB = &DB{domains: make(map[string]*Domain)}

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
		cluster: cluster,
		bucket:  bucket,
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
