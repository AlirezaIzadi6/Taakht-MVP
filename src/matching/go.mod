module github.com/taakht/taakht/src/matching

go 1.27

require (
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.11.0
	github.com/taakht/taakht/gen v0.0.0
	github.com/taakht/taakht/libs/goplatform v0.0.0
	github.com/twmb/franz-go v1.22.1
	google.golang.org/grpc v1.84.0
	google.golang.org/protobuf v1.36.12
)

replace (
	github.com/taakht/taakht/gen => ../../gen/go
	github.com/taakht/taakht/libs/goplatform => ../../libs/goplatform
)
