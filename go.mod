module github.com/nuclio/nuclio

go 1.24.0

require (
	cloud.google.com/go/pubsub v1.2.0
	github.com/Microsoft/ApplicationInsights-Go v0.4.2
	github.com/Shopify/sarama v1.23.1
	github.com/aws/aws-sdk-go v1.30.0
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/disintegration/imaging v1.6.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/fatih/structs v1.1.0
	github.com/ghodss/yaml v1.0.0
	github.com/go-chi/chi v4.0.2+incompatible
	github.com/go-chi/cors v1.0.0
	github.com/hashicorp/go-uuid v1.0.1
	github.com/heptiolabs/healthcheck v0.0.0-20180807145615-6ff867650f40
	github.com/icza/dyno v0.0.0-20180601094105-0c96289f9585
	github.com/imdario/mergo v0.3.7
	github.com/jarcoal/httpmock v1.0.4
	github.com/mgutz/ansi v0.0.0-20170206155736-9520e82c474b
	github.com/mholt/archiver/v3 v3.3.0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/mitchellh/mapstructure v1.1.2
	github.com/nats-io/go-nats v1.7.2
	github.com/nuclio/errors v0.0.3
	github.com/nuclio/logger v0.0.1
	github.com/nuclio/logger-appinsights v0.0.1
	github.com/nuclio/nuclio-sdk-go v0.1.0
	github.com/nuclio/zap v0.0.3
	github.com/olekukonko/tablewriter v0.0.1
	github.com/prometheus/client_golang v1.1.0
	github.com/rcrowley/go-metrics v0.0.0-20181016184325-3113b8401b8a
	github.com/robfig/cron v1.2.0
	github.com/rs/xid v1.2.1
	github.com/satori/go.uuid v1.2.0
	github.com/sendgridlabs/go-kinesis v0.0.0-20190306160747-8de9069567f6
	github.com/spf13/cobra v0.0.5
	github.com/streadway/amqp v0.0.0-20190815230801-eade30b20f1d
	github.com/stretchr/testify v1.5.1
	github.com/tsenart/vegeta v6.3.0+incompatible
	github.com/v3io/scaler-types v1.7.0
	github.com/v3io/v3io-go v0.1.6
	github.com/v3io/v3io-go-http v0.0.1
	github.com/v3io/version-go v0.0.2
	github.com/valyala/fasthttp v1.14.0
	github.com/vmihailenco/msgpack v4.0.4+incompatible
	golang.org/x/sync v0.17.0
	gopkg.in/src-d/go-git.v4 v4.13.1
	gopkg.in/yaml.v2 v2.2.8
	k8s.io/api v0.15.12
	k8s.io/apimachinery v0.15.12
	k8s.io/client-go v0.15.12
	pack.ag/amqp v0.12.5
)

require (
	cloud.google.com/go v0.52.0 // indirect
	code.cloudfoundry.org/clock v1.0.0 // indirect
	github.com/Azure/azure-sdk-for-go v43.3.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest v0.11.0 // indirect
	github.com/Azure/go-autorest/autorest/adal v0.9.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/BurntSushi/toml v0.3.1 // indirect
	github.com/andybalholm/brotli v1.0.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cpuguy83/go-md2man v1.0.10 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dsnet/compress v0.0.1 // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/emirpasic/gods v1.12.0 // indirect
	github.com/gogo/protobuf v1.1.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/golang/protobuf v1.3.3 // indirect
	github.com/golang/snappy v0.0.1 // indirect
	github.com/google/go-cmp v0.6.0 // indirect
	github.com/google/gofuzz v1.0.0 // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/googleapis/gnostic v0.0.0-20170729233727-0c5108395e2d // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/hashicorp/golang-lru v0.5.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jbenet/go-context v0.0.0-20150711004518-d14ea06fba99 // indirect
	github.com/jcmturner/gofork v0.0.0-20190328161633-dc7c13fece03 // indirect
	github.com/jmespath/go-jmespath v0.3.0 // indirect
	github.com/json-iterator/go v1.1.7 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/kevinburke/ssh_config v0.0.0-20190725054713-01f96b0aa0cd // indirect
	github.com/klauspost/compress v1.10.4 // indirect
	github.com/klauspost/pgzip v1.2.1 // indirect
	github.com/mattn/go-colorable v0.1.1 // indirect
	github.com/mattn/go-isatty v0.0.5 // indirect
	github.com/mattn/go-runewidth v0.0.9 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.1 // indirect
	github.com/nats-io/gnatsd v1.4.1 // indirect
	github.com/nats-io/nkeys v0.2.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	github.com/nwaples/rardecode v1.0.0 // indirect
	github.com/pavius/zap v1.4.2-0.20180228181622-8d52692529b8 // indirect
	github.com/pierrec/lz4 v2.2.6+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_model v0.0.0-20190812154241-14fe0d1b01d4 // indirect
	github.com/prometheus/common v0.6.0 // indirect
	github.com/prometheus/procfs v0.0.3 // indirect
	github.com/russross/blackfriday v1.5.2 // indirect
	github.com/sergi/go-diff v1.0.0 // indirect
	github.com/spf13/pflag v1.0.3 // indirect
	github.com/src-d/gcfg v1.4.0 // indirect
	github.com/streadway/quantile v0.0.0-20150917103942-b0c588724d25 // indirect
	github.com/stretchr/objx v0.2.0 // indirect
	github.com/ulikunitz/xz v0.5.6 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/xanzy/ssh-agent v0.2.1 // indirect
	github.com/xi2/xz v0.0.0-20171230120015-48954b6210f8 // indirect
	go.opencensus.io v0.22.3 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/exp v0.0.0-20200119233911-0405dc783f0a // indirect
	golang.org/x/image v0.0.0-20190802002840-cff245a6509b // indirect
	golang.org/x/lint v0.0.0-20200130185559-910be7a94367 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/net v0.44.0 // indirect
	golang.org/x/oauth2 v0.0.0-20200107190931-bf48bf16ab8d // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/term v0.35.0 // indirect
	golang.org/x/text v0.29.0 // indirect
	golang.org/x/time v0.0.0-20191024005414-555d28b269f0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	golang.org/x/tools/go/expect v0.1.1-deprecated // indirect
	google.golang.org/api v0.15.0 // indirect
	google.golang.org/appengine v1.6.5 // indirect
	google.golang.org/genproto v0.0.0-20200204135345-fa8e72b47b90 // indirect
	google.golang.org/grpc v1.27.0 // indirect
	gopkg.in/DATA-DOG/go-sqlmock.v1 v1.3.0 // indirect
	gopkg.in/inf.v0 v0.9.0 // indirect
	gopkg.in/jcmturner/aescts.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/dnsutils.v1 v1.0.1 // indirect
	gopkg.in/jcmturner/gokrb5.v7 v7.2.3 // indirect
	gopkg.in/jcmturner/rpc.v1 v1.1.0 // indirect
	gopkg.in/src-d/go-billy.v4 v4.3.2 // indirect
	gopkg.in/warnings.v0 v0.1.2 // indirect
	honnef.co/go/tools v0.0.1-2019.2.3 // indirect
	k8s.io/klog v0.3.1 // indirect
	k8s.io/utils v0.0.0-20190221042446-c2654d5206da // indirect
	sigs.k8s.io/yaml v1.1.0 // indirect
	zombiezen.com/go/capnproto2 v2.17.0+incompatible // indirect
)

replace github.com/Shopify/sarama => github.com/iguazio/sarama v1.25.1-0.20200331135945-d92101249c96
