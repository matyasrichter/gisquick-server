package processing

import (
	"fmt"
	"strings"
)

// PrefixProcessID adds a service UUID prefix to a process ID.
func PrefixProcessID(serviceID string, processID string) string {
	return serviceID + ":" + processID
}

// ParsePrefixedID parses a prefixed ID (processID or jobID) into service UUID and original ID.
func ParsePrefixedID(prefixedID string) (string, string, error) {
	parts := strings.SplitN(prefixedID, ":", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid prefixed ID format: %s", prefixedID)
	}
	return parts[0], parts[1], nil
}
