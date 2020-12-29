package srclient

import (
	"context"
	"errors"
	"fmt"
	"testing"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var _ Client = (*CachingClient)(nil)

func checkSchemaCache(t *testing.T, client *CachingClient, schema *Schema) {
	val, present := client.cache.infcache.GetIfPresent(fmt.Sprintf(cacheKeySchemaByID, schema.ID))
	require.True(t, present)
	require.Equal(t, schema, val)

	if schema.Version > 0 {
		val, present = client.cache.cache.GetIfPresent(fmt.Sprintf(cacheKeySchemaByVersion, schema.Subject, schema.Version))
		require.True(t, present)
		require.Equal(t, schema, val)
	}

	val, present = client.cache.infcache.GetIfPresent(cachableSchemaFromSchema(schema).Sum64())
	require.True(t, present)
	require.Equal(t, schema, val)
}

func TestCachingClientGetSubjects(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subjects := []string{"sub1", "sub2"}
	c.EXPECT().GetSubjects(ctx).MaxTimes(1).Return(subjects, nil)

	cc := NewCachingClient(c)
	result, err := cc.GetSubjects(ctx)
	require.NoError(t, err)
	require.EqualValues(t, subjects, result)

	val, present := cc.cache.cache.GetIfPresent("subjects")
	require.True(t, present)
	require.EqualValues(t, val, subjects)

	result, err = cc.GetSubjects(ctx)
	require.NoError(t, err)
	require.EqualValues(t, subjects, result)
}

func TestCachingClientGetSubjectsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	c.EXPECT().GetSubjects(ctx).MaxTimes(1).Return([]string{"sub"}, errors.New("err"))

	cc := NewCachingClient(c)
	_, err := cc.GetSubjects(ctx)
	require.Error(t, err)

	_, present := cc.cache.cache.GetIfPresent("subjects")
	require.False(t, present)
}

func TestCachingClientSchemaVersions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subject := "sub"
	versions := []int{1, 2}
	c.EXPECT().GetSubjectVersions(ctx, subject).MaxTimes(1).Return(versions, nil)

	cc := NewCachingClient(c)

	result, err := cc.GetSubjectVersions(ctx, subject)
	require.NoError(t, err)
	require.EqualValues(t, versions, result)

	val, present := cc.cache.cache.GetIfPresent("versions/" + subject)
	require.True(t, present)
	require.EqualValues(t, val, versions)

	result, err = cc.GetSubjectVersions(ctx, subject)
	require.NoError(t, err)
	require.EqualValues(t, versions, result)
}

func TestCachingClientSchemaVersionsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subject := "sub"
	c.EXPECT().GetSubjectVersions(ctx, subject).MaxTimes(1).Return([]int{1, 2}, errors.New("err"))

	cc := NewCachingClient(c)

	_, err := cc.GetSubjectVersions(ctx, subject)
	require.Error(t, err)

	_, present := cc.cache.cache.GetIfPresent("versions/" + subject)
	require.False(t, present)
}

func TestCachingClientSchemaByID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	schema := makeSchema(withRandomID)
	c.EXPECT().GetSchemaByID(ctx, schema.ID).MaxTimes(1).Return(schema, nil)

	cc := NewCachingClient(c)

	result, err := cc.GetSchemaByID(ctx, schema.ID)
	require.NoError(t, err)
	require.Equal(t, schema, result)

	checkSchemaCache(t, cc, schema)

	result, err = cc.GetSchemaByID(ctx, schema.ID)
	require.NoError(t, err)
	require.Equal(t, schema, result)
}

func TestCachingClientSchemaByVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	schema := makeSchema(withRandomID, withRandomVersion)
	c.EXPECT().GetSchemaByVersion(ctx, schema.Subject, schema.Version).MaxTimes(1).Return(schema, nil)

	cc := NewCachingClient(c)

	result, err := cc.GetSchemaByVersion(ctx, schema.Subject, schema.Version)
	require.NoError(t, err)
	require.Equal(t, schema, result)

	checkSchemaCache(t, cc, schema)

	result, err = cc.GetSchemaByVersion(ctx, schema.Subject, schema.Version)
	require.NoError(t, err)
	require.Equal(t, schema, result)
}

func TestCachingClientGetLatestSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	schema := makeSchema(withRandomID, withRandomVersion)
	c.EXPECT().GetLatestSchema(ctx, schema.Subject).MaxTimes(1).Return(schema, nil)

	cc := NewCachingClient(c)

	result, err := cc.GetLatestSchema(ctx, schema.Subject)
	require.NoError(t, err)
	require.Equal(t, schema, result)

	checkSchemaCache(t, cc, schema)

	result, err = cc.GetLatestSchema(ctx, schema.Subject)
	require.NoError(t, err)
	require.Equal(t, schema, result)
}

func TestCachingClientCreateSchema(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	schema := makeSchema(withRandomID, withTestReferences)
	c.EXPECT().CreateSchema(ctx, schema).MaxTimes(1).Return(schema, nil)

	cc := NewCachingClient(c)

	createdSchema, err := cc.CreateSchema(ctx, schema)
	require.NoError(t, err)
	require.Equal(t, schema, createdSchema)

	checkSchemaCache(t, cc, createdSchema)

	createdSchema, err = cc.CreateSchema(ctx, schema)
	require.NoError(t, err)
	require.Equal(t, schema, createdSchema)
}

func TestCachingClientDeleteSubject(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subject := "subject"
	versions := []int{1}
	c.EXPECT().DeleteSubject(ctx, subject, true).Return(versions, nil)

	cc := NewCachingClient(c)
	cc.cache.cache.Put("key", "value")
	cc.cache.infcache.Put("key", "value")

	resultVersions, err := cc.DeleteSubject(ctx, subject, true)
	require.NoError(t, err)
	require.EqualValues(t, versions, resultVersions)

	_, exists := cc.cache.cache.GetIfPresent("key")
	require.False(t, exists)

	_, exists = cc.cache.infcache.GetIfPresent("key")
	require.False(t, exists)
}

func TestCachingClientDeleteSchemaByVersion(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subject := "subject"
	version := 1
	c.EXPECT().DeleteSchemaByVersion(ctx, subject, version, false).Return(version, nil)

	ckSchemaByVersion := fmt.Sprintf(cacheKeySchemaByVersion, subject, version)
	ckSchemaLatests := fmt.Sprintf(cacheKeySchemaLatest, subject)

	cc := NewCachingClient(c)
	cc.cache.cache.Put(ckSchemaByVersion, "value")
	cc.cache.cache.Put(ckSchemaLatests, "value")

	resultVersion, err := cc.DeleteSchemaByVersion(ctx, subject, version, false)
	require.NoError(t, err)
	require.EqualValues(t, version, resultVersion)

	_, exists := cc.cache.cache.GetIfPresent(ckSchemaByVersion)
	require.False(t, exists)

	_, exists = cc.cache.cache.GetIfPresent(ckSchemaLatests)
	require.False(t, exists)
}

func TestCachingClientDeleteSchemaByVersionPersistent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	subject := "subject"
	version := 1
	c.EXPECT().DeleteSchemaByVersion(ctx, subject, version, true).Return(version, nil)

	cc := NewCachingClient(c)
	cc.cache.cache.Put("key", "value")
	cc.cache.infcache.Put("key", "value")

	resultVersion, err := cc.DeleteSchemaByVersion(ctx, subject, version, true)
	require.NoError(t, err)
	require.EqualValues(t, version, resultVersion)

	_, exists := cc.cache.cache.GetIfPresent("key")
	require.False(t, exists)

	_, exists = cc.cache.infcache.GetIfPresent("key")
	require.False(t, exists)
}

func TestCachingClientIsSchemaCompatible(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	c := NewMockClient(ctrl)
	ctx := context.Background()

	schema := makeSchema()
	c.EXPECT().IsSchemaCompatible(ctx, schema).MaxTimes(1).Return(true, nil)

	cc := NewCachingClient(c)

	ok, err := cc.IsSchemaCompatible(ctx, schema)
	require.NoError(t, err)
	require.True(t, ok)
}
