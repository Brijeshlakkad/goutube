// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.1
// source: api/streaming/v1/streaming.proto

package streaming_v1

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type ConsumeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Point string `protobuf:"bytes,1,opt,name=point,proto3" json:"point,omitempty"`
}

func (x *ConsumeRequest) Reset() {
	*x = ConsumeRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_streaming_v1_streaming_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConsumeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConsumeRequest) ProtoMessage() {}

func (x *ConsumeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_streaming_v1_streaming_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConsumeRequest.ProtoReflect.Descriptor instead.
func (*ConsumeRequest) Descriptor() ([]byte, []int) {
	return file_api_streaming_v1_streaming_proto_rawDescGZIP(), []int{0}
}

func (x *ConsumeRequest) GetPoint() string {
	if x != nil {
		return x.Point
	}
	return ""
}

type ConsumeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Frame []byte `protobuf:"bytes,1,opt,name=frame,proto3" json:"frame,omitempty"`
}

func (x *ConsumeResponse) Reset() {
	*x = ConsumeResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_streaming_v1_streaming_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ConsumeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ConsumeResponse) ProtoMessage() {}

func (x *ConsumeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_streaming_v1_streaming_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ConsumeResponse.ProtoReflect.Descriptor instead.
func (*ConsumeResponse) Descriptor() ([]byte, []int) {
	return file_api_streaming_v1_streaming_proto_rawDescGZIP(), []int{1}
}

func (x *ConsumeResponse) GetFrame() []byte {
	if x != nil {
		return x.Frame
	}
	return nil
}

type ProduceRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Point string `protobuf:"bytes,1,opt,name=point,proto3" json:"point,omitempty"`
	Frame []byte `protobuf:"bytes,3,opt,name=frame,proto3" json:"frame,omitempty"`
}

func (x *ProduceRequest) Reset() {
	*x = ProduceRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_streaming_v1_streaming_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProduceRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProduceRequest) ProtoMessage() {}

func (x *ProduceRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_streaming_v1_streaming_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProduceRequest.ProtoReflect.Descriptor instead.
func (*ProduceRequest) Descriptor() ([]byte, []int) {
	return file_api_streaming_v1_streaming_proto_rawDescGZIP(), []int{2}
}

func (x *ProduceRequest) GetPoint() string {
	if x != nil {
		return x.Point
	}
	return ""
}

func (x *ProduceRequest) GetFrame() []byte {
	if x != nil {
		return x.Frame
	}
	return nil
}

type ProduceResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Offset uint64 `protobuf:"varint,1,opt,name=offset,proto3" json:"offset,omitempty"`
}

func (x *ProduceResponse) Reset() {
	*x = ProduceResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_streaming_v1_streaming_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ProduceResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProduceResponse) ProtoMessage() {}

func (x *ProduceResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_streaming_v1_streaming_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProduceResponse.ProtoReflect.Descriptor instead.
func (*ProduceResponse) Descriptor() ([]byte, []int) {
	return file_api_streaming_v1_streaming_proto_rawDescGZIP(), []int{3}
}

func (x *ProduceResponse) GetOffset() uint64 {
	if x != nil {
		return x.Offset
	}
	return 0
}

var File_api_streaming_v1_streaming_proto protoreflect.FileDescriptor

var file_api_streaming_v1_streaming_proto_rawDesc = []byte{
	0x0a, 0x20, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2f,
	0x76, 0x31, 0x2f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x0c, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31,
	0x22, 0x26, 0x0a, 0x0e, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x22, 0x27, 0x0a, 0x0f, 0x43, 0x6f, 0x6e, 0x73,
	0x75, 0x6d, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x66,
	0x72, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x66, 0x72, 0x61, 0x6d,
	0x65, 0x22, 0x3c, 0x0a, 0x0e, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x66, 0x72, 0x61,
	0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x66, 0x72, 0x61, 0x6d, 0x65, 0x22,
	0x29, 0x0a, 0x0f, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x16, 0x0a, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x04, 0x52, 0x06, 0x6f, 0x66, 0x66, 0x73, 0x65, 0x74, 0x32, 0xaf, 0x01, 0x0a, 0x09, 0x53,
	0x74, 0x72, 0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x12, 0x50, 0x0a, 0x0d, 0x43, 0x6f, 0x6e, 0x73,
	0x75, 0x6d, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x1c, 0x2e, 0x73, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x12, 0x50, 0x0a, 0x0d, 0x50, 0x72,
	0x6f, 0x64, 0x75, 0x63, 0x65, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x12, 0x1c, 0x2e, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x72, 0x6f, 0x64, 0x75,
	0x63, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x1d, 0x2e, 0x73, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x69, 0x6e, 0x67, 0x2e, 0x76, 0x31, 0x2e, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x00, 0x28, 0x01, 0x42, 0x2b, 0x5a, 0x29,
	0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x42, 0x72, 0x69, 0x6a, 0x65,
	0x73, 0x68, 0x6c, 0x61, 0x6b, 0x6b, 0x61, 0x64, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x69, 0x6e, 0x67, 0x5f, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x33,
}

var (
	file_api_streaming_v1_streaming_proto_rawDescOnce sync.Once
	file_api_streaming_v1_streaming_proto_rawDescData = file_api_streaming_v1_streaming_proto_rawDesc
)

func file_api_streaming_v1_streaming_proto_rawDescGZIP() []byte {
	file_api_streaming_v1_streaming_proto_rawDescOnce.Do(func() {
		file_api_streaming_v1_streaming_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_streaming_v1_streaming_proto_rawDescData)
	})
	return file_api_streaming_v1_streaming_proto_rawDescData
}

var file_api_streaming_v1_streaming_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_api_streaming_v1_streaming_proto_goTypes = []interface{}{
	(*ConsumeRequest)(nil),  // 0: streaming.v1.ConsumeRequest
	(*ConsumeResponse)(nil), // 1: streaming.v1.ConsumeResponse
	(*ProduceRequest)(nil),  // 2: streaming.v1.ProduceRequest
	(*ProduceResponse)(nil), // 3: streaming.v1.ProduceResponse
}
var file_api_streaming_v1_streaming_proto_depIdxs = []int32{
	0, // 0: streaming.v1.Streaming.ConsumeStream:input_type -> streaming.v1.ConsumeRequest
	2, // 1: streaming.v1.Streaming.ProduceStream:input_type -> streaming.v1.ProduceRequest
	1, // 2: streaming.v1.Streaming.ConsumeStream:output_type -> streaming.v1.ConsumeResponse
	3, // 3: streaming.v1.Streaming.ProduceStream:output_type -> streaming.v1.ProduceResponse
	2, // [2:4] is the sub-list for method output_type
	0, // [0:2] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_api_streaming_v1_streaming_proto_init() }
func file_api_streaming_v1_streaming_proto_init() {
	if File_api_streaming_v1_streaming_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_streaming_v1_streaming_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConsumeRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_streaming_v1_streaming_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ConsumeResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_streaming_v1_streaming_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProduceRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_api_streaming_v1_streaming_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ProduceResponse); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_api_streaming_v1_streaming_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_streaming_v1_streaming_proto_goTypes,
		DependencyIndexes: file_api_streaming_v1_streaming_proto_depIdxs,
		MessageInfos:      file_api_streaming_v1_streaming_proto_msgTypes,
	}.Build()
	File_api_streaming_v1_streaming_proto = out.File
	file_api_streaming_v1_streaming_proto_rawDesc = nil
	file_api_streaming_v1_streaming_proto_goTypes = nil
	file_api_streaming_v1_streaming_proto_depIdxs = nil
}
