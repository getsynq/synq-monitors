package v1beta2

import (
	"fmt"

	sqltestsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/sqltests/v1"
	testsuggestionsv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/datachecks/testsuggestions/v1"
	entitiesv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/entities/v1"
)

// convertSingleTest converts a single YAML test to a SqlTest protobuf
func convertSingleTest(
	yamlTest TestInline,
	monitoredID string,
) (*sqltestsv1.SqlTest, ConversionErrors) {
	var errors ConversionErrors

	proto := &sqltestsv1.SqlTest{
		Name: yamlTest.GetName(),
		Template: &sqltestsv1.Template{
			Identifier: &entitiesv1.Identifier{
				Id: &entitiesv1.Identifier_SynqPath{
					SynqPath: &entitiesv1.SynqPathIdentifier{
						Path: monitoredID,
					},
				},
			},
		},
	}

	// Convert test based on type
	switch t := yamlTest.(type) {
	case *NotNullTest:
		testTemplate, convErrors := convertNotNullTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *UniqueTest:
		testTemplate, convErrors := convertUniqueTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *AcceptedValuesTest:
		testTemplate, convErrors := convertAcceptedValuesTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *RejectedValuesTest:
		testTemplate, convErrors := convertRejectedValuesTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *MinMaxTest:
		testTemplate, convErrors := convertMinMaxTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *MinValueTest:
		testTemplate, convErrors := convertMinValueTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *MaxValueTest:
		testTemplate, convErrors := convertMaxValueTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *FreshnessTest:
		testTemplate, convErrors := convertFreshnessTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *RelativeTimeTest:
		testTemplate, convErrors := convertRelativeTimeTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	case *BusinessRuleTest:
		testTemplate, convErrors := convertBusinessRuleTest(t)
		if len(convErrors) > 0 {
			errors = append(errors, convErrors...)
		} else {
			proto.Template.Test = testTemplate
		}

	default:
		errors = append(errors, ConversionError{
			Field:   "type",
			Message: fmt.Sprintf("unsupported test type: %s", t),
		})
	}

	return proto, errors
}

// convertNotNullTest converts a YAML not_null test to protobuf
func convertNotNullTest(yamlTest *NotNullTest) (*sqltestsv1.Template_NotNullTest, ConversionErrors) {
	var errors ConversionErrors

	if len(yamlTest.Columns) == 0 {
		errors = append(errors, ConversionError{
			Field:   "columns",
			Message: "columns are required for not_null tests",
			Test:    yamlTest.GetId(),
		})
		return nil, errors
	}

	return &sqltestsv1.Template_NotNullTest{
		NotNullTest: &testsuggestionsv1.NotNullTest{
			ColumnNames: yamlTest.Columns,
		},
	}, nil
}

// convertUniqueTest converts a YAML unique test to protobuf
func convertUniqueTest(yamlTest *UniqueTest) (*sqltestsv1.Template_UniqueTest, ConversionErrors) {
	var errors ConversionErrors

	if len(yamlTest.Columns) == 0 {
		errors = append(errors, ConversionError{
			Field:   "columns",
			Message: "columns are required for unique tests",
			Test:    yamlTest.GetId(),
		})
		return nil, errors
	}

	test := &sqltestsv1.Template_UniqueTest{
		UniqueTest: &testsuggestionsv1.UniqueTest{
			ColumnNames:             yamlTest.Columns,
			TimePartitionColumnName: yamlTest.TimePartitionColumn,
		},
	}

	return test, nil
}

// convertAcceptedValuesTest converts a YAML accepted_values test to protobuf
func convertAcceptedValuesTest(yamlTest *AcceptedValuesTest) (*sqltestsv1.Template_AcceptedValuesTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for accepted_values tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(yamlTest.Values) == 0 {
		errors = append(errors, ConversionError{
			Field:   "values",
			Message: "values are required for accepted_values tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_AcceptedValuesTest{
		AcceptedValuesTest: &testsuggestionsv1.AcceptedValuesTest{
			ColumnName:     yamlTest.Column,
			AcceptedValues: yamlTest.Values,
		},
	}, nil
}

// convertRejectedValuesTest converts a YAML rejected_values test to protobuf
func convertRejectedValuesTest(yamlTest *RejectedValuesTest) (*sqltestsv1.Template_RejectedValuesTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for rejected_values tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(yamlTest.Values) == 0 {
		errors = append(errors, ConversionError{
			Field:   "values",
			Message: "values are required for rejected_values tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_RejectedValuesTest{
		RejectedValuesTest: &testsuggestionsv1.RejectedValuesTest{
			ColumnName:     yamlTest.Column,
			RejectedValues: yamlTest.Values,
		},
	}, nil
}

// convertMinMaxTest converts a YAML min_max test to protobuf
func convertMinMaxTest(yamlTest *MinMaxTest) (*sqltestsv1.Template_MinMaxTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for min_max tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_MinMaxTest{
		MinMaxTest: &testsuggestionsv1.MinMaxTest{
			ColumnName: yamlTest.Column,
			MinValue:   yamlTest.MinValue,
			MaxValue:   yamlTest.MaxValue,
		},
	}, nil
}

// convertMinValueTest converts a YAML min_value test to protobuf
func convertMinValueTest(yamlTest *MinValueTest) (*sqltestsv1.Template_MinValueTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for min_value tests",
			Test:    yamlTest.GetId(),
		})
	}
	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_MinValueTest{
		MinValueTest: &testsuggestionsv1.MinValueTest{
			ColumnName: yamlTest.Column,
			MinValue:   yamlTest.MinValue,
		},
	}, nil
}

// convertMaxValueTest converts a YAML max_value test to protobuf
func convertMaxValueTest(yamlTest *MaxValueTest) (*sqltestsv1.Template_MaxValueTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for max_value tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_MaxValueTest{
		MaxValueTest: &testsuggestionsv1.MaxValueTest{
			ColumnName: yamlTest.Column,
			MaxValue:   yamlTest.MaxValue,
		},
	}, nil
}

// convertFreshnessTest converts a YAML freshness test to protobuf
func convertFreshnessTest(yamlTest *FreshnessTest) (*sqltestsv1.Template_FreshnessTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.TimePartitionColumn == "" {
		errors = append(errors, ConversionError{
			Field:   "time_partition_column",
			Message: "time_partition_column is required for freshness tests",
			Test:    yamlTest.GetId(),
		})
	}

	if yamlTest.TimeWindowSeconds == 0 {
		errors = append(errors, ConversionError{
			Field:   "time_window_seconds",
			Message: "time_window_seconds is required for freshness tests and must be greater than 0",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_FreshnessTest{
		FreshnessTest: &testsuggestionsv1.FreshnessTest{
			TimePartitionColumnName: yamlTest.TimePartitionColumn,
			TimeWindowSeconds:       yamlTest.TimeWindowSeconds,
		},
	}, nil
}

// convertRelativeTimeTest converts a YAML relative_time test to protobuf
func convertRelativeTimeTest(yamlTest *RelativeTimeTest) (*sqltestsv1.Template_RelativeTimeTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.Column == "" {
		errors = append(errors, ConversionError{
			Field:   "column",
			Message: "column is required for relative_time tests",
			Test:    yamlTest.GetId(),
		})
	}

	if yamlTest.RelativeColumn == "" {
		errors = append(errors, ConversionError{
			Field:   "relative_column",
			Message: "relative_column is required for relative_time tests",
			Test:    yamlTest.GetId(),
		})
	}

	if len(errors) > 0 {
		return nil, errors
	}

	return &sqltestsv1.Template_RelativeTimeTest{
		RelativeTimeTest: &testsuggestionsv1.RelativeTimeTest{
			ColumnName:         yamlTest.Column,
			RelativeColumnName: yamlTest.RelativeColumn,
		},
	}, nil
}

// convertBusinessRuleTest converts a YAML business_rule test to protobuf
func convertBusinessRuleTest(yamlTest *BusinessRuleTest) (*sqltestsv1.Template_BusinessRuleTest, ConversionErrors) {
	var errors ConversionErrors

	if yamlTest.SQLExpression == "" {
		errors = append(errors, ConversionError{
			Field:   "sql_expression",
			Message: "sql_expression is required for business_rule tests",
			Test:    yamlTest.GetId(),
		})
		return nil, errors
	}

	return &sqltestsv1.Template_BusinessRuleTest{
		BusinessRuleTest: &testsuggestionsv1.BusinessRuleTest{
			SqlExpression: yamlTest.SQLExpression,
		},
	}, nil
}
