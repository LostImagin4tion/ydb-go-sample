package topic

import (
	"bytes"
	"context"
	"fmt"
	"ydb-sample/internal/issue"

	"github.com/google/uuid"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicoptions"
	"github.com/ydb-platform/ydb-go-sdk/v3/topic/topicwriter"
)

type StatusUpdateService struct {
	issueRepo   *issue.IssueRepository
	topicWriter *topicwriter.Writer
}

func NewStatusUpdateService(
	issueRepo *issue.IssueRepository,
	topicClient topic.Client,
) (*StatusUpdateService, error) {
	var topicWriter, err = topicClient.StartWriter(
		"task_status",
		topicoptions.WithWriterProducerID("producer-task-status"),
	)
	if err != nil {
		return nil, err
	}

	return &StatusUpdateService{
		issueRepo: issueRepo,
		topicWriter: topicWriter,
	}, nil
}

func (s *StatusUpdateService) Update(
	ctx context.Context,
	id uuid.UUID,
	status string,
) error {
	var err = s.issueRepo.UpdateStatus(id, status)
	if err != nil {
		return err
	}

	var data = fmt.Sprintf("[%s: %s]", id, status)

	return s.topicWriter.Write(
		ctx,
		topicwriter.Message{
			Data: bytes.NewReader([]byte(data)),
		},
	)
}

func (s *StatusUpdateService) Shutdown(ctx context.Context) error {
	var err = s.topicWriter.Flush(ctx)
	if err != nil {
		return err
	}

	return s.topicWriter.Close(ctx)
}
