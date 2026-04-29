package processing

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gisquick/gisquick-server/internal/domain"
)

// WPSBackend will implement ProcessingBackend for OGC WPS 1.0/2.0 services.
// This is a placeholder stub; the full implementation is provided in the next task.
type WPSBackend struct{}

func (b *WPSBackend) FetchProcessList(_ context.Context, _ domain.ProcessingService) ([]ProcessSummary, error) {
	return nil, fmt.Errorf("WPS backend not yet implemented")
}

func (b *WPSBackend) DescribeProcess(_ context.Context, _ domain.ProcessingService, _ string) (*ProcessDescription, error) {
	return nil, fmt.Errorf("WPS backend not yet implemented")
}

func (b *WPSBackend) Execute(_ context.Context, _ *JobRecord, _ domain.ProcessingService, _ json.RawMessage) ([]OutputResult, string, error) {
	return nil, "", fmt.Errorf("WPS backend not yet implemented")
}
