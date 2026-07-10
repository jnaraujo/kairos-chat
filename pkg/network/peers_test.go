package network

import (
	"reflect"
	"testing"
)

func TestParsePeers(t *testing.T) {
	tests := []struct {
		name       string
		peersRaw   string
		wantMap    map[string]string
		wantList   []string
		wantErr    bool
	}{
		{
			name:     "empty input",
			peersRaw: "",
			wantMap:  map[string]string{},
			wantList: nil,
			wantErr:  false,
		},
		{
			name:     "single peer",
			peersRaw: "userB=localhost:8081",
			wantMap:  map[string]string{"userB": "localhost:8081"},
			wantList: []string{"userB"},
			wantErr:  false,
		},
		{
			name:     "multiple peers",
			peersRaw: "userB=localhost:8081,userC=localhost:8082",
			wantMap:  map[string]string{"userB": "localhost:8081", "userC": "localhost:8082"},
			wantList: []string{"userB", "userC"},
			wantErr:  false,
		},
		{
			name:     "invalid format no equals",
			peersRaw: "userB",
			wantMap:  nil,
			wantList: nil,
			wantErr:  true,
		},
		{
			name:     "invalid format multiple equals",
			peersRaw: "userB=localhost:8081=extra",
			wantMap:  nil,
			wantList: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMap, gotList, err := ParsePeers(tt.peersRaw)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParsePeers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if !reflect.DeepEqual(gotMap, tt.wantMap) {
					t.Errorf("ParsePeers() gotMap = %v, want %v", gotMap, tt.wantMap)
				}
				if !reflect.DeepEqual(gotList, tt.wantList) {
					t.Errorf("ParsePeers() gotList = %v, want %v", gotList, tt.wantList)
				}
			}
		})
	}
}
