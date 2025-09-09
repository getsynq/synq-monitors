module github.com/getsynq/monitors_mgmt

go 1.25.0

replace buf.build/gen/go/getsynq/api/protocolbuffers/go => ../cloud/gen/go/buf.build/gen/go/getsynq/api/protocolbuffers/go

require (
	buf.build/gen/go/getsynq/api/grpc/go v1.5.1-20250813150149-5818d3cb4e3c.2
	github.com/fatih/color v1.18.0
	github.com/google/uuid v1.6.0
	github.com/manifoldco/promptui v0.9.0
	github.com/samber/lo v1.51.0
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.10.0
	golang.org/x/oauth2 v0.30.0
	google.golang.org/grpc v1.74.2
	google.golang.org/protobuf v1.36.8
	gopkg.in/yaml.v3 v3.0.1
)

require (
	cloud.google.com/go/compute/metadata v0.7.0 // indirect
	github.com/chzyer/readline v1.5.1 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/joho/godotenv v1.5.1
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spf13/pflag v1.0.7 // indirect
	golang.org/x/net v0.43.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	golang.org/x/text v0.28.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250811230008-5f3141c8851a // indirect
)

require (
	buf.build/gen/go/getsynq/api/protocolbuffers/go v1.36.1-20250813150149-5818d3cb4e3c.1
	github.com/gkampitakis/go-snaps v0.5.14
)

require (
	github.com/gkampitakis/ciinfo v0.3.2 // indirect
	github.com/gkampitakis/go-diff v1.3.2 // indirect
	github.com/goccy/go-yaml v1.18.0 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/maruel/natural v1.1.1 // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.1 // indirect
	github.com/tidwall/sjson v1.2.5 // indirect
)
