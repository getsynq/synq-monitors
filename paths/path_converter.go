package paths

import (
	"context"
	"fmt"
	"slices"

	"buf.build/gen/go/getsynq/api/grpc/go/synq/entities/coordinates/v1/coordinatesv1grpc"
	"buf.build/gen/go/getsynq/api/grpc/go/synq/entities/entities/v1/entitiesv1grpc"
	coordinatesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/coordinates/v1"
	entitiesentitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/entities/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
	"github.com/samber/lo"
	"google.golang.org/grpc"
)

type PathConverter interface {
	SimpleToPath(simple []string) (map[string]string, *SimpleToPathError)
	PathToSimple(paths []string) (map[string]string, error)
}

type pathConverter struct {
	ctx                context.Context
	entitiesService    entitiesv1grpc.EntitiesServiceClient
	coordinatesService coordinatesv1grpc.DatabaseCoordinatesServiceClient
}

func NewPathConverter(
	ctx context.Context,
	conn *grpc.ClientConn,
) PathConverter {
	return &pathConverter{
		ctx:                ctx,
		entitiesService:    entitiesv1grpc.NewEntitiesServiceClient(conn),
		coordinatesService: coordinatesv1grpc.NewDatabaseCoordinatesServiceClient(conn),
	}
}

// Returns the resolved paths for the given simple paths.
// A simple path is either a DB coordinate or a dot notation path.
// The resolved paths are the Synq paths (with ::) for the given simple paths.
// If a simple path resolves to multiple Synq paths, all of them are returned.
// If a simple path cannot be resolved, it is added to the error.
func (s *pathConverter) SimpleToPath(simple []string) (map[string]string, *SimpleToPathError) {
	if len(simple) == 0 {
		return map[string]string{}, nil
	}

	Err := &SimpleToPathError{
		UnresolvedPaths:                       []string{},
		MonitoredEntitiesWithMultipleEntities: map[string][]string{},
	}
	resolvedPaths := map[string]string{}

	// fetch entities for all paths
	{
		resp, err := s.entitiesService.BatchGetEntities(s.ctx, &entitiesentitiesv1.BatchGetEntitiesRequest{
			Ids: lo.Map(simple, func(path string, _ int) *entitiesv1.Identifier {
				return &entitiesv1.Identifier{
					Id: &entitiesv1.Identifier_SynqPath{
						SynqPath: &entitiesv1.SynqPathIdentifier{
							Path: PathWithColons(path),
						},
					},
				}
			}),
		})
		if err != nil {
			Err.Err = fmt.Errorf("error fetching entities for monitored paths: %w", err)
			return nil, Err
		}

		for _, entity := range resp.Entities {
			if entity.Id.GetSynqPath() != nil {
				resolvedPaths[PathWithDots(entity.Id.GetSynqPath().Path)] = entity.Id.GetSynqPath().Path
			}
		}
	}

	// for the ones without entities, fetch as coordinates
	{
		pathsToFetchCoordinates := lo.Filter(simple, func(path string, _ int) bool {
			_, ok := resolvedPaths[path]
			return !ok
		})
		if len(pathsToFetchCoordinates) > 0 {
			coordResp, err := s.coordinatesService.BatchIdsByCoordinates(s.ctx, &coordinatesv1.BatchIdsByCoordinatesRequest{
				SqlFqn: pathsToFetchCoordinates,
			})
			if err != nil {
				Err.Err = fmt.Errorf("error fetching coordinates for monitored paths: %w", err)
				return nil, Err
			}

			ambiguiousPathsByCoordinate := map[string][]string{}
			for _, coord := range coordResp.MatchedCoordinates {
				if len(coord.Candidates) == 0 || (len(coord.Candidates) == 1 && len(coord.Candidates[0].SynqPaths) == 0) {
					Err.UnresolvedPaths = append(Err.UnresolvedPaths, coord.SqlFqn)
					continue
				}
				if len(coord.Candidates) == 1 && len(coord.Candidates[0].SynqPaths) == 1 {
					resolvedPaths[coord.SqlFqn] = coord.Candidates[0].SynqPaths[0]
					continue
				}
				// more than one possible candidate or synq paths
				// check for ambiguity
				ambiguiousPathsByCoordinate[coord.SqlFqn] = lo.FlatMap(coord.Candidates, func(cand *coordinatesv1.DatabaseCoordinates, _ int) []string {
					return cand.SynqPaths
				})
			}

			allAmbiguousPaths := lo.Uniq(lo.FlatMap(lo.Values(ambiguiousPathsByCoordinate), func(paths []string, _ int) []string {
				return paths
			}))
			if len(allAmbiguousPaths) > 0 {
				// fetch entities for all ambiguous paths and see if they are valid types
				resp, err := s.entitiesService.BatchGetEntities(s.ctx, &entitiesentitiesv1.BatchGetEntitiesRequest{
					Ids: lo.Map(allAmbiguousPaths, func(path string, _ int) *entitiesv1.Identifier {
						return &entitiesv1.Identifier{
							Id: &entitiesv1.Identifier_SynqPath{
								SynqPath: &entitiesv1.SynqPathIdentifier{
									Path: PathWithColons(path),
								},
							},
						}
					}),
				})
				if err != nil {
					Err.Err = fmt.Errorf("error fetching entities for ambiguous paths: %w", err)
					return nil, Err
				}

				typesByPath := map[string]*entitiesv1.EntityType{}
				for _, entity := range resp.Entities {
					typesByPath[entity.SynqPath] = entity.EntityType
				}

				for coord, paths := range ambiguiousPathsByCoordinate {
					validPaths := lo.Filter(paths, func(path string, _ int) bool {
						typ, ok := typesByPath[path]
						return ok && slices.Contains(ValidMonitoredTypes, *typ)
					})
					if len(validPaths) == 1 {
						resolvedPaths[coord] = validPaths[0]
					} else if len(validPaths) > 1 {
						Err.MonitoredEntitiesWithMultipleEntities[coord] = lo.Uniq(validPaths)
					} else {
						Err.UnresolvedPaths = append(Err.UnresolvedPaths, coord)
					}
				}
			}
		}
	}

	if Err.HasErrors() {
		return nil, Err
	}
	return resolvedPaths, nil
}

// Returns the simplified version of the requested paths.
// A simplified version is:
// * a DB coordinate iff the path maps to exactly one DB coordinate
// * else the original path with :: replaced by .
func (s *pathConverter) PathToSimple(paths []string) (map[string]string, error) {
	if len(paths) == 0 {
		return map[string]string{}, nil
	}

	simplifiedPaths := map[string]string{}

	coordResp, err := s.coordinatesService.BatchDatabaseCoordinates(s.ctx, &coordinatesv1.BatchDatabaseCoordinatesRequest{
		Ids: lo.Map(paths, func(path string, _ int) *entitiesv1.Identifier {
			return &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: path,
					},
				},
			}
		}),
	})
	if err != nil {
		return nil, fmt.Errorf("error fetching coordinates for monitored paths: %w", err)
	}

	for _, coord := range coordResp.Coordinates {
		// Use db coordinates only if it maps to a single synq path
		if len(coord.SqlFqn) > 0 && len(coord.SynqPaths) == 1 {
			simplifiedPaths[coord.SynqPaths[0]] = coord.SqlFqn
		}
	}

	for _, path := range paths {
		if _, ok := simplifiedPaths[path]; !ok {
			simplifiedPaths[path] = PathWithDots(path)
		}
	}

	return simplifiedPaths, nil
}
