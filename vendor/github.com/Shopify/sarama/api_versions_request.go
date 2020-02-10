package sarama

//ApiVersionsRequest ...
type ApiVersionsRequest struct {
	Version int16
}

func (a *ApiVersionsRequest) encode(pe packetEncoder) error {
	return nil
}

func (a *ApiVersionsRequest) decode(pd packetDecoder, version int16) (err error) {
	return nil
}

func (a *ApiVersionsRequest) key() int16 {
	return 18
}

func (a *ApiVersionsRequest) version() int16 {
	return a.Version
}

func (a *ApiVersionsRequest) requiredVersion() KafkaVersion {
	return V0_10_0_0
}
