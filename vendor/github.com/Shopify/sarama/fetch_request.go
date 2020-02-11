package sarama

type fetchRequestBlock struct {
	version        int16
	fetchOffset    int64
	logStartOffset int64
	maxBytes       int32
}

type forgottenTopicRequestBlock struct {
	version        int16
	partitions     int32
}

func (b *fetchRequestBlock) encode(pe packetEncoder) error {
	pe.putInt64(b.fetchOffset)
	if b.version >= 5 {
		pe.putInt64(b.logStartOffset)
	}

	pe.putInt32(b.maxBytes)
	return nil
}

func (b *fetchRequestBlock) decode(pd packetDecoder) (err error) {
	if b.fetchOffset, err = pd.getInt64(); err != nil {
		return err
	}
	if b.maxBytes, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

// see https://cwiki.apache.org/confluence/display/KAFKA/KIP-227%3A+Introduce+Incremental+FetchRequests+to+Increase+Partition+Scalability
func (b *forgottenTopicRequestBlock) encode(pe packetEncoder) error {
	pe.putInt32(b.partitions)
	return nil
}

func (b *forgottenTopicRequestBlock) decode(pd packetDecoder) (err error) {
	if b.partitions, err = pd.getInt32(); err != nil {
		return err
	}
	return nil
}

// FetchRequest (API key 1) will fetch Kafka messages. Version 3 introduced the MaxBytes field. See
// https://issues.apache.org/jira/browse/KAFKA-2063 for a discussion of the issues leading up to that.  The KIP is at
// https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes
type FetchRequest struct {
	MaxWaitTime  int32
	MinBytes     int32
	MaxBytes     int32
	Version      int16
	Isolation    IsolationLevel
	SessionID    int32
	SessionEpoch int32
	topicsBlocks map[string]map[int32]*fetchRequestBlock
	forgottenTopicsBlocks map[string]*forgottenTopicRequestBlock
}

type IsolationLevel int8

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
)

func (r *FetchRequest) encode(pe packetEncoder) (err error) {
	pe.putInt32(-1) // replica ID is always -1 for clients
	pe.putInt32(r.MaxWaitTime)
	pe.putInt32(r.MinBytes)
	if r.Version >= 3 {
		pe.putInt32(r.MaxBytes)
	}
	if r.Version >= 4 {
		pe.putInt8(int8(r.Isolation))
	}

	if r.Version >= 7 {
		pe.putInt32(r.SessionID)
		pe.putInt32(r.SessionEpoch)
	}

	err = pe.putArrayLength(len(r.topicsBlocks))
	if err != nil {
		return err
	}
	for topic, blocks := range r.topicsBlocks {
		err = pe.putString(topic)
		if err != nil {
			return err
		}
		err = pe.putArrayLength(len(blocks))
		if err != nil {
			return err
		}
		for partition, block := range blocks {
			pe.putInt32(partition)
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}

	if r.Version >= 7 {
		err = pe.putArrayLength(len(r.forgottenTopicsBlocks))
		if err != nil {
			return err
		}
		for topic, block := range r.forgottenTopicsBlocks {
			err = pe.putString(topic)
			if err != nil {
				return err
			}
			err = block.encode(pe)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *FetchRequest) decode(pd packetDecoder, version int16) (err error) {
	r.Version = version
	if _, err = pd.getInt32(); err != nil {
		return err
	}
	if r.MaxWaitTime, err = pd.getInt32(); err != nil {
		return err
	}
	if r.MinBytes, err = pd.getInt32(); err != nil {
		return err
	}
	if r.Version >= 3 {
		if r.MaxBytes, err = pd.getInt32(); err != nil {
			return err
		}
	}
	if r.Version >= 4 {
		isolation, err := pd.getInt8()
		if err != nil {
			return err
		}
		r.Isolation = IsolationLevel(isolation)
	}
	topicCount, err := pd.getArrayLength()
	if err != nil {
		return err
	}
	if topicCount != 0 {
		r.topicsBlocks = make(map[string]map[int32]*fetchRequestBlock)
		for i := 0; i < topicCount; i++ {
			topic, err := pd.getString()
			if err != nil {
				return err
			}
			partitionCount, err := pd.getArrayLength()
			if err != nil {
				return err
			}
			r.topicsBlocks[topic] = make(map[int32]*fetchRequestBlock)
			for j := 0; j < partitionCount; j++ {
				partition, err := pd.getInt32()
				if err != nil {
					return err
				}
				fetchBlock := &fetchRequestBlock{}
				if err = fetchBlock.decode(pd); err != nil {
					return err
				}
				r.topicsBlocks[topic][partition] = fetchBlock
			}
		}
	}

	if r.Version >= 7 {
		forgottenTopicCount, err := pd.getArrayLength()
		if err != nil {
			return err
		}
		if forgottenTopicCount != 0 {
			r.forgottenTopicsBlocks = make(map[string]*forgottenTopicRequestBlock)
			for i := 0; i < topicCount; i++ {
				topic, err := pd.getString()
				if err != nil {
					return err
				}
				r.forgottenTopicsBlocks[topic] = &forgottenTopicRequestBlock{}
				partitions, err := pd.getInt32()
				if err != nil {
					return err
				}
				r.forgottenTopicsBlocks[topic].partitions = partitions
			}
		}
	}
	return nil
}

func (r *FetchRequest) key() int16 {
	return 1
}

func (r *FetchRequest) version() int16 {
	return r.Version
}

func (r *FetchRequest) requiredVersion() KafkaVersion {
	switch r.Version {
	case 1:
		return V0_9_0_0
	case 2:
		return V0_10_0_0
	case 3:
		return V0_10_1_0
	case 4:
		return V0_11_0_0
	case 7:
		return V1_0_0_0
	default:
		return MinVersion
	}
}

func (r *FetchRequest) AddTopicBlock(version int16,
	topic string,
	partitionID int32,
	fetchOffset int64,
	logStartOffset int64,
	maxBytes int32) {
	if r.topicsBlocks == nil {
		r.topicsBlocks = make(map[string]map[int32]*fetchRequestBlock)
	}

	if r.topicsBlocks[topic] == nil {
		r.topicsBlocks[topic] = make(map[int32]*fetchRequestBlock)
	}

	tmp := new(fetchRequestBlock)
	tmp.version = version
	tmp.maxBytes = maxBytes
	tmp.logStartOffset = logStartOffset
	tmp.fetchOffset = fetchOffset

	r.topicsBlocks[topic][partitionID] = tmp
}

func (r *FetchRequest) AddForgottenTopicBlock(version int16,
	topic string,
	partitions int32) {
	if r.forgottenTopicsBlocks == nil {
		r.forgottenTopicsBlocks = make(map[string]*forgottenTopicRequestBlock)
	}

	forgottenTopic := new(forgottenTopicRequestBlock)
	forgottenTopic.version = version
	forgottenTopic.partitions = partitions

	r.forgottenTopicsBlocks[topic] = forgottenTopic
}