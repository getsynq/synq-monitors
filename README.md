# Custom Monitors Management CLI

Deploy custom monitors from YAML configuration files.

## Script execution

```bash
go run ./... examples/minimal.yaml
```

## Installation

Download the latest release of the built tool: https://github.com/getsynq/synq-monitors/tags.

You can also run the build directly by cloning this repo locally.

Use

```bash
make
```

or

```bash
# Build the CLI
go build -o synq-monitors .
```

## Configuration

**API URL Options:**

- `https://developer.synq.io` - Not US production environment
- `https://api.us.synq.io` - US production environment

The CLI requires Synq API credentials. You can provide them in three ways (in order of priority):

### Option 1: Command Line Flags (Highest Priority)

```bash
./synq-monitors deploy examples/minimal.yaml --client-id="your_client_id" --client-secret="your_client_secret" --api-url="https://developer.synq.io"
```

### Option 2: Environment Variables

```bash
export SYNQ_CLIENT_ID="your_client_id"
export SYNQ_CLIENT_SECRET="your_client_secret"
export SYNQ_API_URL="https://developer.synq.io"
```

### Option 3: .env File

Create a `.env` file in your project root:

```bash
SYNQ_CLIENT_ID=your_client_id
SYNQ_CLIENT_SECRET=your_client_secret
SYNQ_API_URL=https://developer.synq.io
```

**Priority Order**: Command line flags > Environment variables > .env files

## Usage

### Deploy

```bash
./synq-monitors deploy [yaml-file-path] [flags]
```

#### Available Flags

- `--client-id string`: Synq client ID (overrides .env and environment variables)
- `--client-secret string`: Synq client secret (overrides .env and environment variables)
- `--api-url string`: Synq API URL (overrides .env and environment variables)
- `-p, --print-protobuf`: Print protobuf messages in JSON format
- `--auto-confirm`: Automatically confirm all prompts (skip interactive confirmations)
- `-h, --help`: Show help information

#### How it works

1. **Preview**: Shows first 20 lines of YAML file
2. **Confirm**: Asks for confirmation with `y/N` prompt
3. **Convert**: Parses YAML and converts to protobuf MonitorDefinitions
4. **Display**: Shows configuration summary and protobuf JSON output

#### Examples

```bash
# Basic usage
./synq-monitors deploy sample_monitors.yaml

# With command line credentials
./synq-monitors deploy sample_monitors.yaml --client-id="prod_client" --client-secret="prod_secret" --api-url="https://developer.synq.io"

# With protobuf output in JSON format
./synq-monitors deploy sample_monitors.yaml -p

# With auto-confirm (skip all prompts)
./synq-monitors deploy sample_monitors.yaml --auto-confirm
```

### Export

```bash
./synq-monitors export [flags] [output-file]
```

#### Available Flags

- `-h, --help`: Show help information
- `--client-id string`: Synq client ID (overrides .env and environment variables)
- `--client-secret string`: Synq client secret (overrides .env and environment variables)
- `--api-url string`: Synq API URL (overrides .env and environment variables)
- `--namespace string`: Namespace for the config to be exported to. Ensure this is a unique namespace for your config.
- `--integration string`: Integration scope. Limit exported monitors by integration IDs. AND'ed with other scopes.
- `--monitored string`: Monitored asset scope. Limit exported monitors by monitored asset paths. AND'ed with other scopes.
- `--monitor string`: Monitor scope. Limit exported monitors by monitor IDs. AND'ed with other scopes.
- `--source string`: Source scope. Limit exported monitors by source. One of ["app", "api", "all"]. Defaults to "app". AND'ed with other scopes.

#### How it works

Select existing monitors and export them to a YAML file.

1. **Fetch**: Monitors are fetched based on provided scopes.
2. **Validate**: Exported monitors are validated for parsable code.

The output file should not already exist.

#### Examples

```bash
# Basic usage
./synq-monitors export --namespace=all_app_monitors generated/all_app_monitors.yaml

# With command line credentials
./synq-monitors export --namespace=all_app_monitors --client-id="prod_client" --client-secret="prod_secret" --api-url="https://developer.synq.io" generated/all_app_monitors.yaml

# Export monitors on a single table
./synq-monitors export --namespace=runs_table_monitors --monitored="runs-table-path" generated/runs_table_monitors.yaml

# Export monitors on a multiple tables
./synq-monitors export --namespace=runs_monitors --monitored="runs-table-path" --monitored="runs-results-path" generated/runs_table_monitors.yaml
```

## YAML Format

```yaml
namespace: "data-team-pipeline"

defaults:
  severity: ERROR
  timezone: "Europe/Paris" # optional, defaults to UTC if not specified
  daily:
    query_delay: 1h # optional, defaults to 0s if not specified

monitors:
  - name: freshness_on_orders
    time_partitioning: created_at
    type: freshness
    expression: "created_at"
    monitored_ids:
      - orders_table_eu
      - orders_table_us

  - name: volume_on_logs
    time_partitioning: at
    type: volume
    monitored_id: log_table
    segmentation: "country"
    filter: "country IN ('US', 'CA')"

  - name: stats_on_user_fields
    type: field_stats
    time_partitioning: registered_at
    fields:
      - age
      - signup_method
    monitored_id: users_table
    mode:
      anomaly_engine:
        sensitivity: BALANCED
    timezone: "UTC" # optional, defaults to UTC if not specified
    daily:
      query_delay: 0s # midnight (0 seconds since midnight)

  - name: custom_numeric_active_users
    time_partitioning: registered_at
    type: custom_numeric
    metric_aggregation: "COUNT(DISTINCT user_id)"
    monitored_ids:
      - active_users_table
    mode:
      fixed_thresholds:
        min: 100
        max: 10000
    hourly:
      query_delay: 15m # 15 minutes delay for hourly execution
```

## Monitor Types

- **freshness**: Requires `expression`
- **volume**: Basic volume monitoring
- **field_stats**: Requires `fields` array
- **custom_numeric**: Requires `metric_aggregation`

## Optional Features

- **segmentation**: Column to segment by
- **filter**: SQL filter expression
- **mode**: `anomaly_engine` or `fixed_thresholds`
- **schedule**: `daily` or `hourly` with `time_partitioning_shift` (deprecated) or `query_delay`

## Available YAML Fields

### Top-level Configuration

| Field       | Type   | Required | Default | Description                             |
| ----------- | ------ | -------- | ------- | --------------------------------------- |
| `namespace` | string | ❌       | -       | Unique identifier for the configuration |
| `defaults`  | object | ❌       | -       | Default values applied to all monitors  |
| `monitors`  | array  | ✅       | -       | Array of monitor definitions            |

### Defaults Section

| Field                        | Type   | Required | Default                                | Description                                                              |
| ---------------------------- | ------ | -------- | -------------------------------------- | ------------------------------------------------------------------------ |
| `defaults.severity`          | string | ❌       | `ERROR`                                | Default severity level for monitors. Possible values: `WARNING`, `ERROR` |
| `defaults.time_partitioning` | string | ❌       | -                                      | Default time partitioning expression                                     |
| `defaults.daily`             | object | ❌       | -                                      | Default daily schedule configuration                                     |
| `defaults.hourly`            | object | ❌       | -                                      | Default hourly schedule configuration                                    |
| `defaults.mode`              | object | ❌       | `anomaly_engine.sensitivity: BALANCED` | Default detection mode                                                   |
| `defaults.timezone`          | string | ❌       | `UTC`                                  | Default timezone for schedule execution (e.g., "America/New_York")       |

### Monitor Fields

| Field                | Type          | Required | Default                        | Description                                                                           |
| -------------------- | ------------- | -------- | ------------------------------ | ------------------------------------------------------------------------------------- |
| `id`                 | string        | ✅       | -                              | Unique identifier for the monitor                                                     |
| `name`               | string        | ❌       | `{id}`                         | Human-readable monitor name (defaults to monitor id)                                  |
| `type`               | string        | ✅       | -                              | Monitor type. Possible values: `freshness`, `volume`, `custom_numeric`, `field_stats` |
| `expression`         | string        | ❌       | -                              | **Required for `freshness` monitors** - SQL expression to evaluate                    |
| `metric_aggregation` | string        | ❌       | -                              | **Required for `custom_numeric` monitors** - Aggregation function                     |
| `monitored_ids`      | array[string] | ✅❌     | -                              | Array of monitored entity IDs (mutually exclusive with `monitored_id`)                |
| `monitored_id`       | string        | ❌✅     | -                              | Single monitored entity ID (mutually exclusive with `monitored_ids`)                  |
| `fields`             | array[string] | ❌       | -                              | **Required for `field_stats` monitors** - Fields to analyze                           |
| `segmentation`       | object        | ❌       | -                              | Segmentation configuration for the monitor                                            |
| `filter`             | string        | ❌       | -                              | SQL WHERE clause for filtering data                                                   |
| `severity`           | string        | ❌       | `{defaults.severity}`          | Monitor severity level. Possible values: `WARNING`, `ERROR`                           |
| `time_partitioning`  | string        | ❌       | `{defaults.time_partitioning}` | Time partitioning expression                                                          |
| `mode`               | object        | ❌       | `{defaults.mode}`              | Detection mode configuration                                                          |
| `daily`              | object        | ❌       | `{defaults.daily}`             | Daily schedule configuration                                                          |
| `hourly`             | object        | ❌       | `{defaults.hourly}`            | Hourly schedule configuration                                                         |
| `timezone`           | string        | ❌       | `{defaults.timezone}`          | Timezone for schedule execution (e.g., "America/New_York")                            |
| `namespace`          | string        | ❌       | `{namespace}`                  | Override default namespace ID                                                         |

### Segmentation Configuration

| Field                         | Type          | Required | Default | Description                                  |
| ----------------------------- | ------------- | -------- | ------- | -------------------------------------------- |
| `segmentation.expression`     | string        | ✅       | -       | SQL expression for data segmentation column  |
| `segmentation.include_values` | array[string] | ❌       | -       | Specific values to include in segmentation   |
| `segmentation.exclude_values` | array[string] | ❌       | -       | Specific values to exclude from segmentation |

### Mode Configuration

| Field                             | Type    | Required | Default    | Description                                                                                |
| --------------------------------- | ------- | -------- | ---------- | ------------------------------------------------------------------------------------------ |
| `mode.anomaly_engine.sensitivity` | string  | ❌       | `BALANCED` | Sensitivity level for anomaly detection. Possible values: `PRECISE`, `BALANCED`, `RELAXED` |
| `mode.fixed_thresholds.min`       | float64 | ❌       | -          | Minimum threshold value                                                                    |
| `mode.fixed_thresholds.max`       | float64 | ❌       | -          | Maximum threshold value                                                                    |

**Note:** Only one of `anomaly_engine` or `fixed_thresholds` should be specified per mode.

### Daily Schedule Configuration

| Field                           | Type     | Required | Default | Description                                                    |
| ------------------------------- | -------- | -------- | ------- | -------------------------------------------------------------- |
| `daily.time_partitioning_shift` | duration | ❌       | -       | Shift of time partitioning. Deprecated, use timezone instead   |
| `daily.query_delay`             | duration | ❌       | -       | Duration to delay query execution (e.g., "1h", "30m", "2h30m") |
| `daily.ignore_last`             | int32    | ❌       | -       | Ignore the last X days from the query results                  |

### Hourly Schedule Configuration

| Field                            | Type     | Required | Default | Description                                                    |
| -------------------------------- | -------- | -------- | ------- | -------------------------------------------------------------- |
| `hourly.time_partitioning_shift` | duration | ❌       | -       | Shift of time partitioning. Deprecated, use timezone instead   |
| `hourly.query_delay`             | duration | ❌       | -       | Duration to delay query execution (e.g., "1h", "30m", "2h30m") |
| `hourly.ignore_last`             | int32    | ❌       | -       | Ignore the last X hours from the query results                 |

**Note:**

- Only one of `daily` or `hourly` should be specified per monitor
- Duration values use Go duration format (e.g., "1h30m", "45m", "2h")
- Timezone is specified at the monitor or defaults level, not within the schedule object
- If no timezone is specified, UTC is used as the default
- `ignore_last` is used to exclude recent data from analysis (e.g., for daily schedules, `ignore_last: 1` ignores the last day; for hourly schedules, it ignores the last hour)

### Field Requirements by Monitor Type

#### Freshness Monitor

- **Required**: `expression`
- **Optional**: All other fields

#### Volume Monitor

- **Required**: None (uses default configuration)
- **Optional**: All fields

#### Custom Numeric Monitor

- **Required**: `metric_aggregation`
- **Optional**: All other fields

#### Field Stats Monitor

- **Required**: `fields`
- **Optional**: All other fields

### Validation Rules

- `monitored_id` and `monitored_ids` cannot be used together (mutually exclusive)
- Either `monitored_id` or `monitored_ids` should be specified for most monitor types
- Only one schedule type (`daily` or `hourly`) can be specified per monitor
- Only one mode type (`anomaly_engine` or `fixed_thresholds`) can be specified per monitor
- `segmentation.expression` is required if segmentation is configured
- `schedule.delay` can be used with either `daily` or `hourly` schedules
- `filter` should be a SQL WHERE clause string (without the WHERE keyword)

**Note:** Some example files may show filter as a YAML object, but it should be a quoted string containing the SQL condition.

### Data Types Reference

#### YAMLConfig (Root Configuration)

- **Fields**: `namespace`, `defaults`, `monitors`
- **Purpose**: Root structure for YAML configuration files

#### YAMLMonitor

- **Purpose**: Individual monitor definition within the monitors array
- **Required Fields**: `id`, `type`
- **Optional Fields**: All others, with specific requirements based on monitor type

#### YAMLSegmentation

- **Purpose**: Configure data segmentation for monitors
- **Required Fields**: `expression`
- **Optional Fields**: `include_values`, `exclude_values`

#### YAMLMode

- **Purpose**: Configure detection mode (anomaly detection or fixed thresholds)
- **Mutually Exclusive**: `anomaly_engine` OR `fixed_thresholds`

#### YAMLDailySchedule / YAMLHourlySchedule

- **Purpose**: Configure monitor execution schedule
- **Mutually Exclusive**: `daily` OR `hourly`
- **Mutually Exclusive within each**: `time_partitioning_shift` OR `query_delay`
