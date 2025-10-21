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

Refer to `schema.json` for the complete and authoritative specification of all supported fields, types, and validation rules. The schema is the source of truth for what is supported.

### Schema Reference in Your Editor

You can reference the schema inline in your YAML files for IDE support and validation:

```yaml
# Reference schema from local path (relative or absolute)
# yaml-language-server: $schema=../../schemas/v1beta2.json
version: v1beta2
namespace: "data-team-pipeline"

entities:
  - id: orders_table
    time_partitioning_column: created_at
    monitors:
      - id: freshness_on_orders
        type: freshness
        expression: "created_at"

  - id: log_table
    monitors:
      - id: volume_on_logs
        type: volume
        filter: "country IN ('US', 'CA')"
```

The schema also supports URLs, so you can reference it directly from the repository:

```yaml
# yaml-language-server: $schema=https://raw.githubusercontent.com/getsynq/synq-monitors/main/schema.json
```

Additionally, the CLI generates a schema when built, which you can pin in your repository alongside the specific CLI version you're using to ensure consistency:

```bash
./synq-monitors schema > ./schemas/v1beta2.json
```

Then reference it in your YAML files as shown above.
