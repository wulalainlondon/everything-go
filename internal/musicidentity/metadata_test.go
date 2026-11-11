package musicidentity

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSharedNativeMetadataVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/native_music_metadata_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Cases []struct {
			Name, ExpectedTitle, ExpectedArtist, Title, Artist string
			Match                                              bool
		}
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.Cases {
		t.Run(c.Name, func(t *testing.T) {
			if got := MatchMetadata(c.ExpectedTitle, c.ExpectedArtist, c.Title, c.Artist); got != c.Match {
				t.Fatalf("got%v want%v", got, c.Match)
			}
		})
	}
}
