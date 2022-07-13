// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.19.1
// source: api/replication/v1/replication.proto

package replication_v1

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

type ReplicateRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *ReplicateRequest) Reset() {
	*x = ReplicateRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_replication_v1_replication_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplicateRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplicateRequest) ProtoMessage() {}

func (x *ReplicateRequest) ProtoReflect() protoreflect.Message {
	mi := &file_api_replication_v1_replication_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplicateRequest.ProtoReflect.Descriptor instead.
func (*ReplicateRequest) Descriptor() ([]byte, []int) {
	return file_api_replication_v1_replication_proto_rawDescGZIP(), []int{0}
}

type ReplicateResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Locus string `protobuf:"bytes,1,opt,name=locus,proto3" json:"locus,omitempty"`
	Point string `protobuf:"bytes,2,opt,name=point,proto3" json:"point,omitempty"`
	Frame []byte `protobuf:"bytes,3,opt,name=frame,proto3" json:"frame,omitempty"`
}

func (x *ReplicateResponse) Reset() {
	*x = ReplicateResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_api_replication_v1_replication_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReplicateResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReplicateResponse) ProtoMessage() {}

func (x *ReplicateResponse) ProtoReflect() protoreflect.Message {
	mi := &file_api_replication_v1_replication_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReplicateResponse.ProtoReflect.Descriptor instead.
func (*ReplicateResponse) Descriptor() ([]byte, []int) {
	return file_api_replication_v1_replication_proto_rawDescGZIP(), []int{1}
}

func (x *ReplicateResponse) GetLocus() string {
	if x != nil {
		return x.Locus
	}
	return ""
}

func (x *ReplicateResponse) GetPoint() string {
	if x != nil {
		return x.Point
	}
	return ""
}

func (x *ReplicateResponse) GetFrame() []byte {
	if x != nil {
		return x.Frame
	}
	return nil
}

var File_api_replication_v1_replication_proto protoreflect.FileDescriptor

var file_api_replication_v1_replication_proto_rawDesc = []byte{
	0x0a, 0x24, 0x61, 0x70, 0x69, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x2f, 0x76, 0x31, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74,
	0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x22, 0x12, 0x0a, 0x10, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63,
	0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x55, 0x0a, 0x11, 0x52, 0x65,
	0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
	0x14, 0x0a, 0x05, 0x6c, 0x6f, 0x63, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05,
	0x6c, 0x6f, 0x63, 0x75, 0x73, 0x12, 0x14, 0x0a, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x66,
	0x72, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05, 0x66, 0x72, 0x61, 0x6d,
	0x65, 0x32, 0x63, 0x0a, 0x0b, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x12, 0x54, 0x0a, 0x09, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x12, 0x20, 0x2e,
	0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31, 0x2e, 0x52,
	0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a,
	0x21, 0x2e, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x76, 0x31,
	0x2e, 0x52, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x22, 0x00, 0x30, 0x01, 0x42, 0x2d, 0x5a, 0x2b, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62,
	0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x42, 0x72, 0x69, 0x6a, 0x65, 0x73, 0x68, 0x6c, 0x61, 0x6b, 0x6b,
	0x61, 0x64, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x5f, 0x76, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_api_replication_v1_replication_proto_rawDescOnce sync.Once
	file_api_replication_v1_replication_proto_rawDescData = file_api_replication_v1_replication_proto_rawDesc
)

func file_api_replication_v1_replication_proto_rawDescGZIP() []byte {
	file_api_replication_v1_replication_proto_rawDescOnce.Do(func() {
		file_api_replication_v1_replication_proto_rawDescData = protoimpl.X.CompressGZIP(file_api_replication_v1_replication_proto_rawDescData)
	})
	return file_api_replication_v1_replication_proto_rawDescData
}

var file_api_replication_v1_replication_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_api_replication_v1_replication_proto_goTypes = []interface{}{
	(*ReplicateRequest)(nil),  // 0: replication.v1.ReplicateRequest
	(*ReplicateResponse)(nil), // 1: replication.v1.ReplicateResponse
}
var file_api_replication_v1_replication_proto_depIdxs = []int32{
	0, // 0: replication.v1.Replication.Replicate:input_type -> replication.v1.ReplicateRequest
	1, // 1: replication.v1.Replication.Replicate:output_type -> replication.v1.ReplicateResponse
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_api_replication_v1_replication_proto_init() }
func file_api_replication_v1_replication_proto_init() {
	if File_api_replication_v1_replication_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_api_replication_v1_replication_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReplicateRequest); i {
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
		file_api_replication_v1_replication_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReplicateResponse); i {
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
			RawDescriptor: file_api_replication_v1_replication_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_api_replication_v1_replication_proto_goTypes,
		DependencyIndexes: file_api_replication_v1_replication_proto_depIdxs,
		MessageInfos:      file_api_replication_v1_replication_proto_msgTypes,
	}.Build()
	File_api_replication_v1_replication_proto = out.File
	file_api_replication_v1_replication_proto_rawDesc = nil
	file_api_replication_v1_replication_proto_goTypes = nil
	file_api_replication_v1_replication_proto_depIdxs = nil
}