package grpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bojand/ghz/protodesc"
	"github.com/fullstorydev/grpcurl"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"
	"github.com/jhump/protoreflect/dynamic"
	"github.com/jhump/protoreflect/grpcreflect"
	"google.golang.org/grpc"
	rpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
)

const (
	grpcReflectionServiceName = "grpc.reflection.v1alpha.ServerReflection"
)

type ProtoParser interface {
	MarshalRequest(path string, message []byte) (*dynamic.Message, error)
	MarshalResponse(path string, message []byte) (*dynamic.Message, error)
	GetAllPaths() []string
}

type protoParser struct {
	Paths     map[string]struct{}
	Requests  map[string]*desc.MessageDescriptor
	Responses map[string]*desc.MessageDescriptor
}

func NewProtoParserFromReflection(addr string) (_ ProtoParser, err error) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	refClient := grpcreflect.NewClient(ctx, rpb.NewServerReflectionClient(conn))
	reflSource := grpcurl.DescriptorSourceFromServer(ctx, refClient)

	services, err := reflSource.ListServices()
	if err != nil {
		return nil, err
	}

	paths := make(map[string]struct{})
	requests := make(map[string]*desc.MessageDescriptor)
	response := make(map[string]*desc.MessageDescriptor)
	for _, service := range services {
		if service == grpcReflectionServiceName {
			continue
		}
		methods, err := grpcurl.ListMethods(reflSource, service)
		if err != nil {
			return nil, err
		}

		for _, method := range methods {
			methodDesc, err := protodesc.GetMethodDescFromReflect(method, refClient)
			if err != nil {
				return nil, err
			}

			path := fmt.Sprintf("/%s/%s", service, methodDesc.GetName())
			paths[path] = struct{}{}
			requests[path] = methodDesc.GetInputType()
			response[path] = methodDesc.GetOutputType()
		}
	}
	return &protoParser{
		Paths:     paths,
		Requests:  requests,
		Responses: response,
	}, nil
}

func NewProtoParser(importPaths, filenames []string) (_ ProtoParser, err error) {
	requests := make(map[string]*desc.MessageDescriptor)
	response := make(map[string]*desc.MessageDescriptor)

	fileNames, err := protoparse.ResolveFilenames(importPaths, filenames...)
	if err != nil {
		return nil, err
	}
	p := protoparse.Parser{
		ImportPaths:           importPaths,
		IncludeSourceCodeInfo: true,
	}
	parsedFiles, err := p.ParseFiles(fileNames...)
	if err != nil {
		return nil, err
	}

	if len(parsedFiles) < 1 {
		err = errors.New("proto file not found")
		return nil, err
	}

	for _, parsedFile := range parsedFiles {
		for _, service := range parsedFile.GetServices() {
			serviceName := fmt.Sprintf("%s.%s", parsedFile.GetPackage(), service.GetName())
			for _, method := range service.GetMethods() {
				path := fmt.Sprintf("/%s/%s", serviceName, method.GetName())
				requests[path] = method.GetInputType()
				response[path] = method.GetOutputType()
			}
		}
	}
	return &protoParser{
		Requests:  requests,
		Responses: response,
	}, nil
}

func (p *protoParser) MarshalRequest(path string, message []byte) (*dynamic.Message, error) {
	descriptor, ok := p.Requests[path]
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	msg := dynamic.NewMessage(descriptor)
	return msg, msg.Unmarshal(message)
}

func (p *protoParser) MarshalResponse(path string, message []byte) (*dynamic.Message, error) {
	descriptor, ok := p.Responses[path]
	if !ok {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	msg := dynamic.NewMessage(descriptor)
	return msg, msg.Unmarshal(message)
}

func (p *protoParser) GetAllPaths() []string {
	paths := make([]string, 0, len(p.Paths))
	for path := range p.Paths {
		paths = append(paths, path)
	}
	return paths
}
