package v1beta1

import (
	"fmt"
	"strings"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
)

// convertSingleTest converts a single YAML test to a TestSuggestion protobuf
func convertSingleTest(
	yamlTest *YAMLTest,
	config *YAMLConfig,
	monitoredID string,
) (*sqltestsv1.SqlTest, ConversionErrors) {
	var errors ConversionErrors

	testID := strings.TrimSpace(yamlTest.Id)
	configID := yamlTest.ConfigID
	if configID == "" {
		configID = config.ID
	}

	proto := &sqltestsv1.SqlTest{
		Name: 
		Identifier: &entitiesv1.Identifier{
			Id: &entitiesv1.Identifier_SynqPath{
				SynqPath: &entitiesv1.SynqPathIdentifier{
					Path: fmt.Sprintf("%s/%s", configID, testID),
				},
			},
		},
		EntitySynqPath: &monitoredID,
	}

	// Convert test based on type
	switch yamlTest.Type {
	case "not_null":
		test, convErrors := convertNotNullTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_NotNullTest{NotNullTest: test}
		}

	case "unique":
		test, convErrors := convertUniqueTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_UniqueTest{UniqueTest: test}
		}

	case "accepted_values":
		test, convErrors := convertAcceptedValuesTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_AcceptedValuesTest{AcceptedValuesTest: test}
		}

	case "rejected_values":
		test, convErrors := convertRejectedValuesTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_RejectedValuesTest{RejectedValuesTest: test}
		}

	case "min_max":
		test, convErrors := convertMinMaxTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_MinMaxTest{MinMaxTest: test}
		}

	case "min_value":
		test, convErrors := convertMinValueTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_MinValueTest{MinValueTest: test}
		}

	case "max_value":
		test, convErrors := convertMaxValueTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_MaxValueTest{MaxValueTest: test}
		}

	case "freshness":
		test, convErrors := convertFreshnessTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_FreshnessTest{FreshnessTest: test}
		}

	case "relative_time":
		test, convErrors := convertRelativeTimeTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_RelativeTimeTest{RelativeTimeTest: test}
		}

	case "business_rule":
		test, convErrors := convertBusinessRuleTest(yamlTest)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Test = &testsuggestionsv1.TestSuggestion_BusinessRuleTest{BusinessRuleTest: test}
		}

	default:
		errors = append(errors, ConversionError{
			Field:   "type",
			Message: fmt.Sprintf("unsupported test type: %s", yamlTest.Type),
			Test:    testID,
		})
	}

	return proto, errors
}

// convertNotNullTest converts a YAML not_null test to protobuf
func convertNotNullTest(yamlTest *YAMLTest) (*testsuggestionsv1.NotNullTest, ConversionErrors) {
	var errors ConversionErrors

	if len(yamlTest.Columns) == 0 {
		errors = append(errors, ConversionError{
			Field:   "columns",
			Message: "columns are required for not_null tests",
			Test:    yamlTest.Id,
		})
		return nil, errors
	}

	return &testsuggestionsv1.NotNullTest{
		ColumnNames: yamlTest.Columns,
	}, nil
}

// convertUniqueTest converts a YAML unique test to protobuf
func convertUniqueTest(yamlTest *YAMLTest) (*testsuggestionsv1.UniqueTest, ConversionErrors) {
	var errors ConversionErrors

	if len(yamlTest.Columns) == 0 {
		errors = append(errors, ConversionError{
			Field:   "columns",
			Message: "columns are required for unique tests",
			Test:    yamlTest.Id,
		})
		return nil, errors
	}

	test := &testsuggestionsv1.UniqueTest{
		ColumnNames:             yamlTest.Columns,
		TimePartitionColumnName: yamlTest.TimePartitionColumn,
	}

	if yamlTest.TimeWindowSeconds != nil {
		test.TimeWindowSeconds = *yamlTest.TimeWindowSeconds
	}

	return test, nil
}

// convertAcceptedValuesTest converts a YAML accepted_values test to protobuf
func convertAcceptedValuesTest(yamlTest *YAMLTest) (*testsuggestionsv1.AcceptedValuesTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for accepted_values tests",
			Test:    yamlTest.Id,
		})
	}

	if len(yamlTest.Values) == 0 {
		errors = append(errors, ConversionError{
			Field:   "values",
			Message: "values are required for accepted_values tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.AcceptedValuesTest{
		ColumnName:     yamlTest.Column,
		AcceptedValues: yamlTest.Values,
	}, nil
}

// convertRejectedValuesTest converts a YAML rejected_values test to protobuf
func convertRejectedValuesTest(yamlTest *YAMLTest) (*testsuggestionsv1.RejectedValuesTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for rejected_values tests",
			Test:    yamlTest.Id,
		})
	}

	if len(yamlTest.Values) == 0 {
		errors = append(errors, ConversionError{
			Field:   "values",
			Message: "values are required for rejected_values tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.RejectedValuesTest{
		ColumnName:     yamlTest.Column,
		RejectedValues: yamlTest.Values,
	}, nil
}

// convertMinMaxTest converts a YAML min_max test to protobuf
func convertMinMaxTest(yamlTest *YAMLTest) (*testsuggestionsv1.MinMaxTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for min_max tests",
			Test:    yamlTest.Id,
		})
	}

	if yamlTest.MinValue == nil && yamlTest.MaxValue == nil {
		errors = append(errors, ConversionError{
			Field:   "min_value/max_value",
			Message: "at least one of min_value or max_value is required for min_max tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	test := &testsuggestionsv1.MinMaxTest{
		ColumnName: yamlTest.Column,
	}

	if yamlTest.MinValue != nil {
		test.MinValue = *yamlTest.MinValue
	}

	if yamlTest.MaxValue != nil {
		test.MaxValue = *yamlTest.MaxValue
	}

	return test, nil
}

// convertMinValueTest converts a YAML min_value test to protobuf
func convertMinValueTest(yamlTest *YAMLTest) (*testsuggestionsv1.MinValueTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for min_value tests",
			Test:    yamlTest.Id,
		})
	}

	if yamlTest.MinValue == nil {
		errors = append(errors, ConversionError{
			Field:   "min_value",
			Message: "min_value is required for min_value tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.MinValueTest{
		ColumnName: yamlTest.Column,
		MinValue:   *yamlTest.MinValue,
	}, nil
}

// convertMaxValueTest converts a YAML max_value test to protobuf
func convertMaxValueTest(yamlTest *YAMLTest) (*testsuggestionsv1.MaxValueTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for max_value tests",
			Test:    yamlTest.Id,
		})
	}

	if yamlTest.MaxValue == nil {
		errors = append(errors, ConversionError{
			Field:   "max_value",
			Message: "max_value is required for max_value tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.MaxValueTest{
		ColumnName: yamlTest.Column,
		MaxValue:   *yamlTest.MaxValue,
	}, nil
}

// convertFreshnessTest converts a YAML freshness test to protobuf
func convertFreshnessTest(yamlTest *YAMLTest) (*testsuggestionsv1.FreshnessTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.TimePartitionColumn == "" {
		errors = append(errors, ConversionError{
			Field:   "time_partition_column",
			Message: "time_partition_column is required for freshness tests",
			Test:    yamlTest.Id,
		})
	}

	if yamlTest.TimeWindowSeconds == nil {
		errors = append(errors, ConversionError{
			Field:   "time_window_seconds",
			Message: "time_window_seconds is required for freshness tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.FreshnessTest{
		TimePartitionColumnName: yamlTest.TimePartitionColumn,
		TimeWindowSeconds:       *yamlTest.TimeWindowSeconds,
	}, nil
}

// convertRelativeTimeTest converts a YAML relative_time test to protobuf
func convertRelativeTimeTest(yamlTest *YAMLTest) (*testsuggestionsv1.RelativeTimeTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for relative_time tests",
			Test:    yamlTest.Id,
		})
	}

	if yamlTest.RelativeColumn == "" {
		errors = append(errors, ConversionError{
			Field:   "relative_column",
			Message: "relative_column is required for relative_time tests",
			Test:    yamlTest.Id,
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &testsuggestionsv1.RelativeTimeTest{
		ColumnName:         yamlTest.Column,
		RelativeColumnName: yamlTest.RelativeColumn,
	}, nil
}

// convertBusinessRuleTest converts a YAML business_rule test to protobuf
func convertBusinessRuleTest(yamlTest *YAMLTest) (*testsuggestionsv1.BusinessRuleTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.SqlExpression == "" {
		errors = append(errors, ConversionError{
			Field:   "sql_expression",
			Message: "sql_expression is required for business_rule tests",
			Test:    yamlTest.Id,
		})
		return nil, errors
	}

	return &testsuggestionsv1.BusinessRuleTest{
		SqlExpression: yamlTest.SqlExpression,
	}, nil
}

// validateTestScheduleConfiguration validates test schedule configuration
func validateTestScheduleConfiguration(test *YAMLTest) ConversionErrors {
	var errors ConversionErrors

	if test.Daily != nil && test.Hourly != nil {
		errors = append(errors, ConversionError{
			Field:   "schedule",
			Message: "daily and hourly schedules are mutually exclusive",
			Test:    test.Id,
		})
		return errors
	}

	if test.Daily != nil {
		if test.Daily.TimePartitioningShift != nil && test.Daily.QueryDelay != nil {
			errors = append(errors, ConversionError{
				Field:   "daily",
				Message: "time_partitioning_shift and query_delay are mutually exclusive within daily schedule",
				Test:    test.Id,
			})
		}
	}

	if test.Hourly != nil {
		if test.Hourly.TimePartitioningShift != nil && test.Hourly.QueryDelay != nil {
			errors = append(errors, ConversionError{
				Field:   "hourly",
				Message: "time_partitioning_shift and query_delay are mutually exclusive within hourly schedule",
				Test:    test.Id,
			})
		}
	}

	return errors
}
