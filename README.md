# Custom Monitors Management CLI

Deploy custom monitors from YAML configuration files.

## Script execution

```bash
go run ./.. sample_config.yaml
```

## Installation

```bash
# Build the CLI
go build -o monitor-mgmt .
```

## Configuration

The CLI requires Synq API credentials. You can provide them in three ways (in order of priority):

### Option 1: Command Line Flags (Highest Priority)

```bash
./monitors-mgmt sample_config.yaml --client-id="your_client_id" --client-secret="your_client_secret"
```

### Option 2: Environment Variables

```bash
export SYNQ_CLIENT_ID="your_client_id"
export SYNQ_CLIENT_SECRET="your_client_secret"
```

### Option 3: .env File

Create a `.env` file in your project root:

```bash
SYNQ_CLIENT_ID=your_client_id
SYNQ_CLIENT_SECRET=your_client_secret
```

**Priority Order**: Command line flags > Environment variables > .env files

## Usage

```bash
./monitors-mgmt [yaml-file-path] [flags]
```

### Available Flags

- `--client-id string`: Synq client ID (overrides .env and environment variables)
- `--client-secret string`: Synq client secret (overrides .env and environment variables)
- `-p, --print-protobuf`: Print protobuf messages in JSON format
- `--auto-confirm`: Automatically confirm all prompts (skip interactive confirmations)
- `-h, --help`: Show help information

### How it works

1. **Preview**: Shows first 20 lines of YAML file
2. **Confirm**: Asks for confirmation with `y/N` prompt
3. **Convert**: Parses YAML and converts to protobuf MonitorDefinitions
4. **Display**: Shows configuration summary and protobuf JSON output

### Examples

```bash
# Basic usage
./monitors-mgmt sample_monitors.yaml

# With command line credentials
./monitors-mgmt sample_monitors.yaml --client-id="prod_client" --client-secret="prod_secret"

# With protobuf output in JSON format
./monitors-mgmt sample_monitors.yaml -p

# With auto-confirm (skip all prompts)
./monitors-mgmt sample_monitors.yaml --auto-confirm
```

## YAML Format

```yaml
config_id: "data-team-pipeline"

defaults:
  severity: ERROR

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
    schedule:
      daily: 0

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
    schedule:
      hourly: 15
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
- **schedule**: `daily` (hour 0-23) or `hourly` (minute 0-59)

## Available YAML Fields

### Top-level Configuration

| Field       | Type   | Required | Default | Description                             |
| ----------- | ------ | -------- | ------- | --------------------------------------- |
| `config_id` | string | ❌       | -       | Unique identifier for the configuration |
| `defaults`  | object | ❌       | -       | Default values applied to all monitors  |
| `monitors`  | array  | ✅       | -       | Array of monitor definitions            |

### Defaults Section

| Field                        | Type   | Required | Default                    | Description                          |
| ---------------------------- | ------ | -------- | -------------------------- | ------------------------------------ |
| `defaults.severity`          | string | ❌       | `ERROR`                    | Default severity level for monitors  |
| `defaults.time_partitioning` | string | ❌       | -                          | Default time partitioning expression |
| `defaults.schedule`          | object | ❌       | `daily: 0`                 | Default schedule configuration       |
| `defaults.mode`              | object | ❌       | `anomaly_engine: balanced` | Default detection mode               |

### Monitor Fields

| Field                | Type          | Required | Default                        | Description                                                            |
| -------------------- | ------------- | -------- | ------------------------------ | ---------------------------------------------------------------------- |
| `id`                 | string        | ✅       | -                              | Unique identifier for the monitor                                      |
| `name`               | string        | ❌       | `{id}`                         | Human-readable monitor name                                            |
| `type`               | string        | ✅       | -                              | Monitor type: `freshness`, `volume`, `custom_numeric`, `field_stats`   |
| `expression`         | string        | ❌       | -                              | **Required for `freshness` monitors** - SQL expression to evaluate     |
| `metric_aggregation` | string        | ❌       | -                              | **Required for `custom_numeric` monitors** - Aggregation function      |
| `monitored_ids`      | array[string] | ✅❌     | -                              | Array of monitored entity IDs (mutually exclusive with `monitored_id`) |
| `monitored_id`       | string        | ❌✅     | -                              | Single monitored entity ID (mutually exclusive with `monitored_ids`)   |
| `fields`             | array[string] | ❌       | -                              | **Required for `field_stats` monitors** - Fields to analyze            |
| `segmentation`       | string        | ❌       | -                              | SQL expression for data segmentation                                   |
| `filter`             | string        | ❌       | -                              | SQL WHERE clause for filtering data                                    |
| `severity`           | string        | ❌       | `{defaults.severity}`          | Monitor severity: `WARNING`, `ERROR`                                   |
| `time_partitioning`  | string        | ✅       | `{defaults.time_partitioning}` | Time partitioning expression                                           |
| `mode`               | object        | ❌       | `{defaults.mode}`              | Detection mode configuration                                           |
| `schedule`           | object        | ❌       | `{defaults.schedule}`          | Schedule configuration                                                 |
| `config_id`          | string        | ❌       | `{config_id}`                  | Override default config ID                                             |

### Mode Configuration

| Field                             | Type    | Required | Default    | Description                                         |
| --------------------------------- | ------- | -------- | ---------- | --------------------------------------------------- |
| `mode.anomaly_engine.sensitivity` | string  | ❌       | `BALANCED` | Sensitivity level: `PRECISE`, `BALANCED`, `RELAXED` |
| `mode.fixed_thresholds.min`       | float64 | ❌       | -          | Minimum threshold value                             |
| `mode.fixed_thresholds.max`       | float64 | ❌       | -          | Maximum threshold value                             |

### Schedule Configuration

| Field             | Type | Required | Default | Description                                         |
| ----------------- | ---- | -------- | ------- | --------------------------------------------------- |
| `schedule.daily`  | int  | ❌       | `0`     | Minutes since midnigth (0-1439) for daily execution |
| `schedule.hourly` | int  | ❌       | -       | Minute of hour (0-59) for hourly execution          |

**Note:** Only one of `daily` or `hourly` should be specified per schedule.

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

- `monitored_id` and `monitored_ids` cannot be used together
- At least one of `monitored_id` or `monitored_ids` must be specified
- Monitor `id` must be unique across all monitors (across all configs)
- `time_partitioning` is required for all monitors
- Only one schedule type (`daily` or `hourly`) can be specified per monitor
