package photosrv

import (
	"reflect"
	"strings"
	"testing"

	"github.com/dgryski/go-simpleconf"
)

// provide consistent access to simpleconf.Config data
func configData() (simpleconf.Config, error) {
	// TODO(rbastic): clean up tests
	config := strings.NewReader(
		`<photosrv>

		<lhr4>
		<shards>
		shard1 http://dmichellis1-base6.dev.booking.com http://dmichellis2-base6.dev.booking.com
		shard2 http://dmichellis3-base6.dev.booking.com http://dmichellis4-base6.dev.booking.com
		shard3 http://dmichellis5-base6.dev.booking.com http://dmichellis6-base6.dev.booking.com
		</shards>
		</lhr4>

		<prefix_rewrite>
		# Existing rules/rewrites/symlinks
		/images/hotel/org/      /images/hotel/max500/
		/images/hotel/partner/  /images/hotel/affiliate/

		# Test cases
		images/hotel/rule-no-lead-slash             /images/hotel/bad-prefix/       # bad prefix  - no leading slash
		/images/hotel/replace-no-lead-slash/        images/hotel/bad-replace/       # bad replace - no leading slash
		/images/hotel/a/                            /images/hotel/c/                # a -> c ...
		/images/hotel/a/                            /images/hotel/b/                # && a -> b
		/images/hotel/rule-no-trail-slash           /images/hotel/dont-do-that/     # questionable: ..a[b]  -> ..c/    ->> c//[b]..
		/images/hotel/replace-no-trail-slash/       /images/hotel/dont-do-that      # questionable: ..a/[b] -> ..c     ->> b[c]...
		/images/hotel/foo                           /images/hotel/bar               # questionable - but allowed
		/broken/ #just no
		</prefix_rewrite>

		</photosrv>`,
	)

	return simpleconf.NewFromReader(config)
}

func TestParseRewrites(t *testing.T) {
	conf, err := configData()
	if err != nil {
		t.Skip("Error in test config - rewrite parsing skipped.", err)
	}
	expected := map[string]string{
		"/images/hotel/org/":     "/images/hotel/max500/",
		"/images/hotel/partner/": "/images/hotel/affiliate/",
		"/images/hotel/foo":      "/images/hotel/bar",
	}

	if rewrites := parseRewrites(conf); !reflect.DeepEqual(rewrites, expected) {
		t.Errorf("error parsing rewrites: %v != %v", rewrites, expected)
	}
}

func TestNormalizeURL(t *testing.T) {

	tests := []struct {
		path       string
		normalized string
	}{
		{"/images/hello.jpg", "/images/hello.jpg"},
		{"/images/hello.jpg?query=foo", "/images/hello.jpg"},
		{"/images//hello.jpg", "/images/hello.jpg"},
		{"/images/./hello.jpg", "/"},
		{"/images/../hello.jpg", "/"},
		{"/images/.jpg", "/"},
		{"/images/hello%2Ejpg", "/images/hello.jpg"},
		{"/images//hello%2Ejpg?query=foo", "/images/hello.jpg"},
	}

	for _, tt := range tests {
		if n := normalizeURL(tt.path); n != tt.normalized {
			t.Errorf("normalizeURL(%v)=%v, want %v", tt.path, n, tt.normalized)
		}
	}
}

func TestRewriteURL(t *testing.T) {

	// rewrites loaded from test config
	conf, err := configData()
	if err != nil {
		t.Skip("Error loading rewrites from test config - rewrite tests will be skipped.", err)
	}
	rewrites := parseRewrites(conf)

	tests := []struct {
		path       string
		normalized string
	}{
		// Rewrites needed in production
		// /images/hotel/org -> max500
		{"/images/hotel/org/hello.jpg", "/images/hotel/max500/hello.jpg"},
		{"/images/hotel/orge/hello.jpg", "/images/hotel/orge/hello.jpg"},
		// /images/hotel/partner -> affiliate
		{"/images/hotel/affiliate/hello.jpg", "/images/hotel/affiliate/hello.jpg"},
		{"/images/hotel/partner/hello.jpg", "/images/hotel/affiliate/hello.jpg"},

		// Error and special cases
		// rewrite must lead with slash -> ignored
		{"/images/hotel/rule-no-lead-slash/hello.jpg", "/images/hotel/rule-no-lead-slash/hello.jpg"},
		{"/images/hotel/replace-no-lead-slash/hello.jpg", "/images/hotel/replace-no-lead-slash/hello.jpg"},
		// both must end in / if one does -> ignored
		{"/images/hotel/rule-no-trail-slash/hello.jpg", "/images/hotel/rule-no-trail-slash/hello.jpg"},
		{"/images/hotel/replace-no-trail-slash/hello.jpg", "/images/hotel/replace-no-trail-slash/hello.jpg"},
		// cannot specify 2 different replacements (a -> b) && (a -> c) -> ignored
		{"/images/hotel/a/hello.jpg", "/images/hotel/a/hello.jpg"},
		// must specifiy a replacement (broken -> {}) -> ignored
		{"/images/hotel/broken/hello.jpg", "/images/hotel/broken/hello.jpg"},
		// /images/hotel/foo -> /images/hotel/bar
		{"/images/hotel/foobuz/hello.jpg", "/images/hotel/barbuz/hello.jpg"},
	}

	for _, tt := range tests {
		if n := rewriteURL(tt.path, rewrites); n != tt.normalized {
			t.Errorf("normalizeURL(%v)=%v, want %v", tt.path, n, tt.normalized)
		}
	}
}

func TestRoster(t *testing.T) {

	tests := []struct {
		roster string
		hosts  map[string]bool
	}{
		{
			"",
			map[string]bool{},
		},
		{
			"\n",
			map[string]bool{},
		},

		{
			`
127.0.0.1 host1
127.0.0.2 host2
`,
			map[string]bool{"host1": true, "host2": true},
		},
		{
			`
127.0.0.1 host1 extra-fields ignored
127.0.0.2 host2 extra-fields still-ignored
`,
			map[string]bool{"host1": true, "host2": true},
		},
	}

	for _, tt := range tests {
		if m := loadRosterFile(strings.NewReader(tt.roster)); !reflect.DeepEqual(m, tt.hosts) {
			t.Errorf("loadRosterFile(%q)=%v, want %v", tt.roster, m, tt.hosts)

		}
	}
}
