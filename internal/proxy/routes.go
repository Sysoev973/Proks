package proxy

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"

	"proks/internal/events"

	"gopkg.in/yaml.v3"
)

type Route struct {
	Match string   `yaml:"match" json:"match"`
	Tags  []string `yaml:"tags" json:"tags"`
}

type routesFile struct {
	Routes []Route `yaml:"routes"`
}

func LoadRoutes(file string) ([]Route, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("read routes: %w", err)
	}
	var parsed routesFile
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		return nil, fmt.Errorf("parse routes %s: %w", file, err)
	}
	if _, err := compileRoutes(parsed.Routes); err != nil {
		return nil, fmt.Errorf("routes %s: %w", file, err)
	}
	return parsed.Routes, nil
}

type compiledRoute struct {
	match    string
	segments []string
	tags     []string
}

type routeTable []compiledRoute

func compileRoutes(routes []Route) (routeTable, error) {
	table := make(routeTable, 0, len(routes))
	for _, route := range routes {
		if !strings.HasPrefix(route.Match, "/") {
			return nil, fmt.Errorf("route %q: match must start with /", route.Match)
		}
		segments := splitPath(route.Match)
		params := make(map[string]bool)
		for _, segment := range segments {
			if name, ok := placeholder(segment); ok {
				if name == "" || params[name] {
					return nil, fmt.Errorf("route %q: invalid or repeated placeholder %q", route.Match, segment)
				}
				params[name] = true
			}
		}
		for _, tag := range route.Tags {
			sample := expandTag(tag, func(name string) (string, bool) {
				if params[name] || strings.HasPrefix(name, "query.") && len(name) > len("query.") {
					return "x", true
				}
				return "", false
			})
			if sample == "" || !events.ValidTag(sample) {
				return nil, fmt.Errorf("route %q: invalid tag template %q", route.Match, tag)
			}
		}
		table = append(table, compiledRoute{match: route.Match, segments: segments, tags: route.Tags})
	}
	return table, nil
}

func (t routeTable) tagsFor(r *http.Request) []string {
	if len(t) == 0 {
		return nil
	}
	segments := splitPath(r.URL.Path)
	for _, route := range t {
		params, ok := route.matchSegments(segments)
		if !ok {
			continue
		}
		var tags []string
		for _, template := range route.tags {
			tag := expandTag(template, func(name string) (string, bool) {
				if query, ok := strings.CutPrefix(name, "query."); ok {
					v := r.URL.Query().Get(query)
					return v, v != ""
				}
				v, ok := params[name]
				return v, ok && v != ""
			})
			if tag != "" && events.ValidTag(tag) {
				tags = append(tags, tag)
			}
		}
		return tags
	}
	return nil
}

// template returns the first route matching path, its template and the
// values of its placeholders.
func (t routeTable) template(path string) (match string, params map[string]string, ok bool) {
	segments := splitPath(path)
	for _, route := range t {
		if params, ok := route.matchSegments(segments); ok {
			return route.match, params, true
		}
	}
	return "", nil, false
}

func (t routeTable) byMatch(match string) (compiledRoute, bool) {
	for _, route := range t {
		if route.match == match {
			return route, true
		}
	}
	return compiledRoute{}, false
}

// expand fills the route's placeholders from params; it fails when one of
// them has no value.
func (r compiledRoute) expand(params map[string]string) (string, bool) {
	out := make([]string, len(r.segments))
	for i, segment := range r.segments {
		if name, ok := placeholder(segment); ok {
			v := params[name]
			if v == "" {
				return "", false
			}
			segment = v
		}
		out[i] = segment
	}
	return "/" + strings.Join(out, "/"), true
}

// sameParams reports whether every placeholder value of next is also in prev
// under the same name, so the next path can be rebuilt from prev's values.
func sameParams(prev, next map[string]string) bool {
	for name, v := range next {
		if prev[name] != v {
			return false
		}
	}
	return true
}

func (r compiledRoute) matchSegments(segments []string) (map[string]string, bool) {
	if len(segments) != len(r.segments) {
		return nil, false
	}
	var params map[string]string
	for i, want := range r.segments {
		if name, ok := placeholder(want); ok {
			if params == nil {
				params = make(map[string]string)
			}
			params[name] = segments[i]
			continue
		}
		if segments[i] != want {
			return nil, false
		}
	}
	return params, true
}

func placeholder(segment string) (string, bool) {
	if strings.HasPrefix(segment, "{") && strings.HasSuffix(segment, "}") {
		return segment[1 : len(segment)-1], true
	}
	return "", false
}

func expandTag(template string, lookup func(name string) (string, bool)) string {
	var out strings.Builder
	for {
		start := strings.IndexByte(template, '{')
		if start < 0 {
			out.WriteString(template)
			return out.String()
		}
		end := strings.IndexByte(template[start:], '}')
		if end < 0 {
			return ""
		}
		value, ok := lookup(template[start+1 : start+end])
		if !ok {
			return ""
		}
		out.WriteString(template[:start])
		out.WriteString(value)
		template = template[start+end+1:]
	}
}

func splitPath(p string) []string {
	trimmed := strings.Trim(p, "/")
	if trimmed == "" {
		return nil
	}
	return strings.Split(trimmed, "/")
}

// maxResponseTags bounds the tags one response can carry.
const maxResponseTags = 64

func responseTags(headers http.Header) []string {
	var tags []string
	for _, value := range headers.Values("Surrogate-Key") {
		tags = append(tags, strings.Fields(value)...)
	}
	for _, name := range []string{"Cache-Tag", "X-Cache-Depends-On"} {
		for _, value := range headers.Values(name) {
			for _, tag := range strings.Split(value, ",") {
				tags = append(tags, strings.TrimSpace(tag))
			}
		}
	}
	return tags
}

func (t routeTable) tagsForResponse(r *http.Request, headers http.Header) []string {
	if r == nil {
		return nil
	}
	var out []string
	seen := make(map[string]bool)
	for _, tag := range append(t.tagsFor(r), responseTags(headers)...) {
		if len(out) == maxResponseTags {
			break
		}
		if seen[tag] || !events.ValidTag(tag) {
			continue
		}
		seen[tag] = true
		out = append(out, tag)
	}
	return out
}

func routeKey(p string) string {
	if p == "" {
		return "/"
	}
	return path.Clean("/" + p)
}
