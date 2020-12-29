package srclient

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/goburrow/cache"
)

const (
	cacheKeySubjects        = "subjects"
	cacheKeySchemaByID      = "id/%d"
	cacheKeySchemaByVersion = "version/%s/%d"
	cacheKeySchemaLatest    = "version/%s/latest"
	cacheKeySchemaVersions  = "versions/%s"
)

type cachableSchema struct {
	Subject    string
	Schema     string
	References []Reference
}

func cachableSchemaFromSchema(s *Schema) *cachableSchema {
	c := &cachableSchema{}
	c.Subject = s.Subject
	c.Schema = s.Schema
	c.References = s.References
	return c
}

// Sum64 create hash over fields that represent unique value of this schema
// and are used for caching
func (c *cachableSchema) Sum64() uint64 {
	h := fnv.New64()

	h.Write([]byte(c.Subject))
	h.Write([]byte(c.Schema))

	for _, ref := range c.References {
		h.Write([]byte(ref.Name))
		h.Write([]byte(ref.Subject))
		h.Write([]byte(strconv.Itoa(ref.Version)))
	}

	return h.Sum64()
}

type cacheHelper struct {
	cache            cache.Cache
	infcache         cache.Cache
	cacheSchemaValue bool
}

type cacheFunc func(val interface{})

func (c *cacheHelper) GetSubjects() ([]string, cacheFunc) {
	cacheFunc := c.defaultCacheFunc(cacheKeySubjects)

	val := []string{}
	if v, exists := c.cache.GetIfPresent(cacheKeySubjects); exists {
		val = v.([]string)
	}

	return val, cacheFunc
}

func (c *cacheHelper) GetSchemaVersions(subject string) ([]int, cacheFunc) {
	key := fmt.Sprintf(cacheKeySchemaVersions, subject)
	cacheFunc := c.defaultCacheFunc(key)

	val := []int{}
	if v, exists := c.cache.GetIfPresent(key); exists {
		val = v.([]int)
	}

	return val, cacheFunc
}

func (c *cacheHelper) GetSchemaByID(schemaID int) (*Schema, cacheFunc) {
	key := fmt.Sprintf(cacheKeySchemaByID, schemaID)
	return c.cacheSchema(key, c.infcache, false)
}

func (c *cacheHelper) GetSchemaByVersion(subject string, version int) (*Schema, cacheFunc) {
	key := fmt.Sprintf(cacheKeySchemaByVersion, subject, version)
	return c.cacheSchema(key, c.cache, false)
}

func (c *cacheHelper) GetLatestSchema(subject string) (*Schema, cacheFunc) {
	key := fmt.Sprintf(cacheKeySchemaLatest, subject)
	return c.cacheSchema(key, c.cache, true)
}

func (c *cacheHelper) GetSchemaValue(schema *Schema) (*Schema, cacheFunc) {
	if c.cacheSchemaValue {
		return c.cacheSchema(cachableSchemaFromSchema(schema).Sum64(), c.infcache, false)
	}

	return nil, c.schemaCacheFunc
}

func (c *cacheHelper) InvalidateSubject(subject string, parmanent bool) {
	c.cache.InvalidateAll()
	c.infcache.InvalidateAll()
	return
}

func (c *cacheHelper) InvalidateSchemaByVersion(subject string, version int, parmanent bool) {
	if parmanent {
		c.cache.InvalidateAll()
		c.infcache.InvalidateAll()
		return
	}

	c.cache.Invalidate(fmt.Sprintf(cacheKeySchemaByVersion, subject, version))
	c.cache.Invalidate(fmt.Sprintf(cacheKeySchemaLatest, subject))
}

func (c *cacheHelper) defaultCacheFunc(key string) cacheFunc {
	return func(val interface{}) {
		c.cache.Put(key, val)
	}
}

func (c *cacheHelper) schemaCacheFunc(val interface{}) {
	schema := val.(*Schema)

	// if schema id is set, cache schema under schema by id key
	if schema.ID > 0 {
		c.infcache.Put(fmt.Sprintf(cacheKeySchemaByID, schema.ID), schema)
	}

	// if schema subject and version is set, cache schema under schema by version key
	if schema.Subject != "" && schema.Version > 0 {
		c.cache.Put(fmt.Sprintf(cacheKeySchemaByVersion, schema.Subject, schema.Version), schema)
	}

	if c.cacheSchemaValue {
		c.infcache.Put(cachableSchemaFromSchema(schema).Sum64(), schema)
	}
}

func (c *cacheHelper) cacheSchema(key interface{}, cache cache.Cache, latest bool) (*Schema, cacheFunc) {
	var val *Schema
	if v, exists := cache.GetIfPresent(key); exists {
		val = v.(*Schema)
	}

	return val, func(val interface{}) {
		schema := val.(*Schema)

		c.cache.Put(fmt.Sprintf(cacheKeySchemaLatest, schema.Subject), schema)
		c.schemaCacheFunc(val)
	}
}

type CachingClientOption func(*CachingClient)

func (CachingClientOption) OptionType() {}

// WithExpiration set expiration for mutable entries like list of subjects
func WithExpiration(time time.Duration) CachingClientOption {
	return func(c *CachingClient) {
		c.expiration = time
	}
}

// WithSchemaValueCaching enables caching of schema values
func WithSchemaValueCaching(enable ...bool) CachingClientOption {
	return func(c *CachingClient) {
		c.cache.cacheSchemaValue = enableOpt(enable)
	}
}

var defaultCachingClientOpts = []CachingClientOption{
	WithSchemaValueCaching(),
}

// CachingClient implements schema registry Client with
// caching capabilities
type CachingClient struct {
	client Client
	cache  cacheHelper

	expiration time.Duration
}

// NewCachingClient creates a new client with caching support
func NewCachingClient(client Client, opts ...CachingClientOption) *CachingClient {
	if client == nil {
		panic("client must be set")
	}

	c := &CachingClient{client: client}

	// apply default caching client options
	for _, opt := range defaultCachingClientOpts {
		opt(c)
	}

	// apply options
	for _, opt := range opts {
		opt(c)
	}

	// initialize cache
	if c.expiration > 0 {
		c.cache.cache = cache.New(cache.WithExpireAfterWrite(c.expiration))
	} else {
		c.cache.cache = cache.New()
	}

	// initialize infinity cache
	c.cache.infcache = cache.New()

	return c
}

// GetSubjects gets a list of defines subjects
func (c *CachingClient) GetSubjects(ctx context.Context) (subjects []string, err error) {
	var cache cacheFunc

	if subjects, cache = c.cache.GetSubjects(); len(subjects) == 0 {
		subjects, err = c.client.GetSubjects(ctx)
		if err == nil {
			cache(subjects)
		}
	}

	return
}

func (c *CachingClient) GetSubjectVersions(ctx context.Context, subject string) (versions []int, err error) {
	var cache cacheFunc

	if versions, cache = c.cache.GetSchemaVersions(subject); len(versions) == 0 {
		versions, err = c.client.GetSubjectVersions(ctx, subject)
		if err == nil {
			cache(versions)
		}
	}

	return
}

func (c *CachingClient) GetSchemaByID(ctx context.Context, schemaID int) (schema *Schema, err error) {
	var cache cacheFunc

	if schema, cache = c.cache.GetSchemaByID(schemaID); schema == nil {
		schema, err = c.client.GetSchemaByID(ctx, schemaID)
		if err == nil {
			cache(schema)
		}
	}

	return
}

func (c *CachingClient) GetSchemaByVersion(ctx context.Context, subject string, version int) (schema *Schema, err error) {
	var cache cacheFunc

	if schema, cache = c.cache.GetSchemaByVersion(subject, version); schema == nil {
		schema, err = c.client.GetSchemaByVersion(ctx, subject, version)
		if err == nil {
			cache(schema)
		}
	}

	return
}

func (c *CachingClient) GetLatestSchema(ctx context.Context, subject string) (schema *Schema, err error) {
	var cache cacheFunc

	if schema, cache = c.cache.GetLatestSchema(subject); schema == nil {
		schema, err = c.client.GetLatestSchema(ctx, subject)
		if err == nil {
			cache(schema)
		}
	}

	return
}

func (c *CachingClient) CreateSchema(ctx context.Context, schema *Schema) (createdSchema *Schema, err error) {
	var cache cacheFunc

	if createdSchema, cache = c.cache.GetSchemaValue(schema); createdSchema == nil {
		createdSchema, err = c.client.CreateSchema(ctx, schema)
		if err == nil {
			cache(schema)
		}
	}

	return
}

func (c *CachingClient) DeleteSubject(ctx context.Context, subject string, permanent bool) ([]int, error) {
	c.cache.InvalidateSubject(subject, permanent)
	return c.client.DeleteSubject(ctx, subject, permanent)
}

func (c *CachingClient) DeleteSchemaByVersion(ctx context.Context, subject string, version int, permanent bool) (int, error) {
	c.cache.InvalidateSchemaByVersion(subject, version, permanent)
	return c.client.DeleteSchemaByVersion(ctx, subject, version, permanent)
}

func (c *CachingClient) IsSchemaCompatible(ctx context.Context, schema *Schema) (bool, error) {
	return c.client.IsSchemaCompatible(ctx, schema)
}

func (c *CachingClient) GetSchemaSubjectVersions(ctx context.Context, schemaID int) (map[string]int, error) {
	return c.client.GetSchemaSubjectVersions(ctx, schemaID)
}
