package paths

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"

	coordinatesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/coordinates/v1"
	entitiesentitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/entities/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	"github.com/getsynq/monitors_mgmt/paths/mocks"
)

func TestPathConverterTestSuite(t *testing.T) {
	suite.Run(t, new(PathConverterTestSuite))
}

type PathConverterTestSuite struct {
	suite.Suite
	ctrl            *gomock.Controller
	mockEntities    *mocks.MockEntitiesServiceClient
	mockCoordinates *mocks.MockDatabaseCoordinatesServiceClient
	converter       *pathConverter
}

func (s *PathConverterTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.mockEntities = mocks.NewMockEntitiesServiceClient(s.ctrl)
	s.mockCoordinates = mocks.NewMockDatabaseCoordinatesServiceClient(s.ctrl)
	s.converter = &pathConverter{
		ctx:                context.Background(),
		entitiesService:    s.mockEntities,
		coordinatesService: s.mockCoordinates,
	}
}

func (s *PathConverterTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *PathConverterTestSuite) TestSimpleToPath() {
	s.Run("entity_found", func() {
		input := []string{"foo.bar"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{
			Entities: []*entitiesv1.Entity{
				{
					Id: &entitiesv1.Identifier{
						Id: &entitiesv1.Identifier_SynqPath{
							SynqPath: &entitiesv1.SynqPathIdentifier{Path: "foo::bar"},
						},
					},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil)
		// No coordinates call expected
		result, err := s.converter.SimpleToPath(input)
		s.Require().Nil(err)
		s.Require().Equal(map[string]string{"foo.bar": "foo::bar"}, result)
	})

	s.Run("coordinate_found", func() {
		input := []string{"db.table"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{Entities: []*entitiesv1.Entity{}}
		coordResp := &coordinatesv1.BatchIdsByCoordinatesResponse{
			MatchedCoordinates: []*coordinatesv1.BatchIdsByCoordinatesResponse_MatchedCoordinates{
				{
					SqlFqn:     "db.table",
					Candidates: []*coordinatesv1.DatabaseCoordinates{{SynqPaths: []string{"integration::db::table"}}},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil)
		s.mockCoordinates.EXPECT().BatchIdsByCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.SimpleToPath(input)
		s.Require().Nil(err)
		s.Require().Equal(map[string]string{"db.table": "integration::db::table"}, result)
	})

	s.Run("entity_or_coordinate_not_found", func() {
		input := []string{"notfound"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{Entities: []*entitiesv1.Entity{}}
		coordResp := &coordinatesv1.BatchIdsByCoordinatesResponse{
			MatchedCoordinates: []*coordinatesv1.BatchIdsByCoordinatesResponse_MatchedCoordinates{
				{
					SqlFqn:     "notfound",
					Candidates: []*coordinatesv1.DatabaseCoordinates{},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil)
		s.mockCoordinates.EXPECT().BatchIdsByCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.SimpleToPath(input)
		s.Require().Empty(result)
		s.Require().Contains(err.UnresolvedPaths, "notfound")
	})

	s.Run("coordinate_found_with_ambiguous_candidates", func() {
		input := []string{"ambiguous2"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{Entities: []*entitiesv1.Entity{
			{
				SynqPath:   "ambiguous2::a",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_CLICKHOUSE_VIEW.Enum(),
			},
			{
				SynqPath:   "ambiguous2::b",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_CLICKHOUSE_TABLE.Enum(),
			},
			{
				SynqPath:   "ambiguous2::c",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_DBT_MODEL.Enum(),
			},
		}}
		coordResp := &coordinatesv1.BatchIdsByCoordinatesResponse{
			MatchedCoordinates: []*coordinatesv1.BatchIdsByCoordinatesResponse_MatchedCoordinates{
				{
					SqlFqn: "ambiguous2",
					Candidates: []*coordinatesv1.DatabaseCoordinates{
						{SynqPaths: []string{"ambiguous2::a", "ambiguous2::b"}},
						{SynqPaths: []string{"ambiguous2::c"}},
					},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
		s.mockCoordinates.EXPECT().BatchIdsByCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.SimpleToPath(input)
		s.Require().Nil(result)
		s.Require().Empty(err.UnresolvedPaths)
		s.Require().Contains(err.MonitoredEntitiesWithMultipleEntities, "ambiguous2")
		s.Require().Len(err.MonitoredEntitiesWithMultipleEntities["ambiguous2"], 2)
		s.Require().Contains(err.MonitoredEntitiesWithMultipleEntities["ambiguous2"], "ambiguous2::a")
		s.Require().Contains(err.MonitoredEntitiesWithMultipleEntities["ambiguous2"], "ambiguous2::b")
	})

	s.Run("coordinate_found_with_unambiguous_candidates", func() {
		input := []string{"ambiguous2"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{Entities: []*entitiesv1.Entity{
			{
				SynqPath:   "ambiguous2::a",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_CLICKHOUSE_VIEW.Enum(),
			},
			{
				SynqPath:   "ambiguous2::b",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_DBT_SNAPSHOT.Enum(),
			},
			{
				SynqPath:   "ambiguous2::c",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_DBT_MODEL.Enum(),
			},
		}}
		coordResp := &coordinatesv1.BatchIdsByCoordinatesResponse{
			MatchedCoordinates: []*coordinatesv1.BatchIdsByCoordinatesResponse_MatchedCoordinates{
				{
					SqlFqn: "ambiguous2",
					Candidates: []*coordinatesv1.DatabaseCoordinates{
						{SynqPaths: []string{"ambiguous2::a", "ambiguous2::b"}},
						{SynqPaths: []string{"ambiguous2::c"}},
					},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
		s.mockCoordinates.EXPECT().BatchIdsByCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.SimpleToPath(input)
		s.Require().Nil(err)
		s.Require().Equal(map[string]string{"ambiguous2": "ambiguous2::a"}, result)
	})

	s.Run("coordinate_found_with_no_valid_candidates", func() {
		input := []string{"ambiguous2"}
		resp := &entitiesentitiesv1.BatchGetEntitiesResponse{Entities: []*entitiesv1.Entity{
			{
				SynqPath:   "ambiguous2::a",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_AIRFLOW_TASK.Enum(),
			},
			{
				SynqPath:   "ambiguous2::b",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_DBT_SNAPSHOT.Enum(),
			},
			{
				SynqPath:   "ambiguous2::c",
				EntityType: entitiesv1.EntityType_ENTITY_TYPE_DBT_MODEL.Enum(),
			},
		}}
		coordResp := &coordinatesv1.BatchIdsByCoordinatesResponse{
			MatchedCoordinates: []*coordinatesv1.BatchIdsByCoordinatesResponse_MatchedCoordinates{
				{
					SqlFqn: "ambiguous2",
					Candidates: []*coordinatesv1.DatabaseCoordinates{
						{SynqPaths: []string{"ambiguous2::a", "ambiguous2::b"}},
						{SynqPaths: []string{"ambiguous2::c"}},
					},
				},
			},
		}
		s.mockEntities.EXPECT().BatchGetEntities(gomock.Any(), gomock.Any()).Return(resp, nil).Times(2)
		s.mockCoordinates.EXPECT().BatchIdsByCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.SimpleToPath(input)
		s.Require().Nil(result)
		s.Require().Empty(err.MonitoredEntitiesWithMultipleEntities)
		s.Require().Len(err.UnresolvedPaths, 1)
		s.Require().Contains(err.UnresolvedPaths, "ambiguous2")
	})
}

func (s *PathConverterTestSuite) TestPathToSimple_CoordinateFound() {
	s.Run("coordinate_found", func() {

		input := []string{"integration::db::table"}
		coordResp := &coordinatesv1.BatchDatabaseCoordinatesResponse{
			Coordinates: []*coordinatesv1.DatabaseCoordinates{
				{
					SqlFqn:    "db.table",
					SynqPaths: []string{"integration::db::table"},
				},
			},
		}
		s.mockCoordinates.EXPECT().BatchDatabaseCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.PathToSimple(input)
		s.Require().Equal(map[string]string{"integration::db::table": "db.table"}, result)
		s.Require().NoError(err)
	})

	s.Run("coordinate_not_found", func() {
		input := []string{"foo::bar"}
		coordResp := &coordinatesv1.BatchDatabaseCoordinatesResponse{
			Coordinates: []*coordinatesv1.DatabaseCoordinates{},
		}
		s.mockCoordinates.EXPECT().BatchDatabaseCoordinates(gomock.Any(), gomock.Any()).Return(coordResp, nil)
		result, err := s.converter.PathToSimple(input)
		s.Require().Equal(map[string]string{"foo::bar": "foo.bar"}, result)
		s.Require().NoError(err)
	})
}
