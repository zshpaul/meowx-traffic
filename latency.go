package main

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

var errLatencyResponse = errors.New("代理列表格式无效")

type delayResult struct {
	Time  time.Time `json:"time"`
	Delay *int      `json:"delay"`
}
type proxyState struct {
	History []delayResult `json:"history"`
}
type latencyProxy struct {
	Name    string                `json:"name"`
	Type    string                `json:"type"`
	Now     string                `json:"now"`
	TestURL string                `json:"testUrl"`
	All     []string              `json:"all"`
	Extra   map[string]proxyState `json:"extra"`
}
type LatencyPoint struct {
	Endpoint   string    `json:"endpoint"`
	Group      string    `json:"group"`
	Node       string    `json:"node"`
	TestURL    string    `json:"testUrl"`
	Time       time.Time `json:"time"`
	Delay      *int      `json:"delay"`
	Segment    string    `json:"segment"`
	Fresh      bool      `json:"fresh"`
	ObservedAt time.Time `json:"observedAt,omitempty"`
}
type latencySelection struct {
	node, testURL, segment string
	since                  time.Time
}

type boundLatencySelection struct {
	scope, segment string
	since          time.Time
}

type LatencyGroup struct {
	Endpoint string   `json:"endpoint"`
	Group    string   `json:"group"`
	Node     string   `json:"node"`
	TestURL  string   `json:"testUrl"`
	Members  []string `json:"members,omitempty"`
}

// ingestLatency records only the latest result of the node selected at observation.
// A timestamp does not establish which node was selected when that test ran.
func (m *Monitor) ingestLatency(base string, proxies map[string]latencyProxy, providers map[string]struct {
	Proxies []latencyProxy `json:"proxies"`
}, now time.Time) {
	m.mu.Lock()
	generation := m.generation
	m.mu.Unlock()
	m.ingestLatencyGeneration(base, proxies, providers, now, generation)
}

func (m *Monitor) ingestLatencyGeneration(base string, proxies map[string]latencyProxy, providers map[string]struct {
	Proxies []latencyProxy `json:"proxies"`
}, now time.Time, generation uint64) {
	m.mu.Lock()
	type boundPoint struct {
		u *Uploader
		p LatencyPoint
	}
	var bound []boundPoint
	defer func() {
		m.mu.Unlock()
		for _, item := range bound {
			item.u.enqueue(item.p)
		}
	}()
	// Ignore an in-flight request after a controller change or disconnect.
	if !m.connected || m.base != base || m.generation != generation {
		return
	}
	if m.latencySegment == "" {
		m.latencySegment = now.UTC().Format(time.RFC3339Nano)
	}
	candidates := map[string][]latencyProxy{}
	for name, p := range proxies {
		if p.Name == "" {
			p.Name = name
		}
		candidates[p.Name] = append(candidates[p.Name], p)
	}
	for _, provider := range providers {
		for _, p := range provider.Proxies {
			candidates[p.Name] = append(candidates[p.Name], p)
		}
	}
	// Binding samples are independent of the group's selected-node chart.
	// Establish a fresh observation boundary for each binding on each connection.
	if m.boundLatency == nil {
		m.boundLatency = make(map[*Uploader]boundLatencySelection)
	}
	uploaders := []*Uploader{}
	if m.uploader != nil {
		uploaders = append(uploaders, m.uploader)
	}
	for _, b := range m.extraBindings {
		uploaders = append(uploaders, b.uploader)
	}
	for _, u := range uploaders {
		s := u.store.Get().Upload
		g, ok := proxies[s.Group]
		member := false
		for _, name := range g.All {
			member = member || name == s.TargetNode
		}
		if !s.Enabled || s.Source != base || s.Binding == "" || s.TargetNode == "" ||
			s.TargetURL == "" || !ok || g.Type != "URLTest" || g.TestURL != s.TargetURL || !member {
			delete(m.boundLatency, u)
			continue
		}
		var state *proxyState
		var encoded string
		ambiguous := false
		for _, p := range candidates[s.TargetNode] {
			switch p.Type {
			case "", "Unknown", "URLTest", "Selector", "Fallback", "LoadBalance", "Relay":
				ambiguous = true
			}
			if len(p.All) != 0 || p.Now != "" {
				ambiguous = true
			}
			v, exists := p.Extra[s.TargetURL]
			if !exists {
				ambiguous = true
				continue
			}
			b, _ := json.Marshal([]any{p.Type, v})
			if state != nil && encoded != string(b) {
				ambiguous = true
			}
			copy := v
			state, encoded = &copy, string(b)
		}
		if ambiguous || state == nil {
			delete(m.boundLatency, u)
			continue
		}
		scopeBytes, _ := json.Marshal([]any{base, s.Group, s.TargetNode, s.TargetURL, s.Binding, s.StartedAt, m.latencySegment})
		scope := string(scopeBytes)
		selection, ok := m.boundLatency[u]
		if !ok || selection.scope != scope {
			selection = boundLatencySelection{scope, m.latencySegment + "/binding/" + now.UTC().Format(time.RFC3339Nano), now}
			m.boundLatency[u] = selection
		}
		history := append([]delayResult(nil), state.History...)
		sort.SliceStable(history, func(i, j int) bool { return history[i].Time.Before(history[j].Time) })
		for _, h := range history {
			if h.Time.IsZero() || h.Time.Before(selection.since) || h.Time.Before(s.StartedAt) ||
				h.Time.After(now.Add(time.Minute)) || h.Delay == nil || *h.Delay < 0 {
				continue
			}
			bound = append(bound, boundPoint{u, LatencyPoint{base, s.Group, s.TargetNode, s.TargetURL, h.Time, h.Delay, selection.segment, true, now}})
		}
	}
	seen := map[string]bool{}
	key := func(p LatencyPoint) string {
		b, _ := json.Marshal([]any{p.Endpoint, p.Group, p.Node, p.TestURL, p.Time.UTC().Format(time.RFC3339Nano)})
		return string(b)
	}
	for _, p := range m.disk.Latency {
		seen[key(p)] = true
	}
	groups := []LatencyGroup{}
	if m.latencySelections == nil {
		m.latencySelections = map[string]latencySelection{}
	}
	for name, g := range proxies {
		if g.Type != "URLTest" {
			continue
		}
		groups = append(groups, LatencyGroup{Endpoint: base, Group: name, Node: g.Now, TestURL: g.TestURL, Members: append([]string(nil), g.All...)})
		selection, ok := m.latencySelections[name]
		if !ok || selection.node != g.Now || selection.testURL != g.TestURL {
			selection = latencySelection{g.Now, g.TestURL, now.UTC().Format(time.RFC3339Nano), now}
			m.latencySelections[name] = selection
		}
		if g.Now == "" || g.TestURL == "" {
			continue
		}
		// Distinct same-named candidates cannot be reliably associated with a group.
		var chosen *latencyProxy
		var encoded string
		ambiguous := false
		for _, p := range candidates[g.Now] {
			state, ok := p.Extra[g.TestURL]
			if !ok {
				continue
			}
			b, _ := json.Marshal(state)
			if chosen != nil && string(b) != encoded {
				ambiguous = true
				break
			}
			cp := p
			chosen = &cp
			encoded = string(b)
		}
		if ambiguous || chosen == nil {
			continue
		}
		history := chosen.Extra[g.TestURL].History
		var latest *delayResult
		for i := range history {
			h := &history[i]
			if h.Time.IsZero() || h.Time.After(now.Add(time.Minute)) {
				continue
			}
			if latest == nil || h.Time.After(latest.Time) {
				latest = h
			}
		}
		if latest == nil || latest.Delay == nil || *latest.Delay < 0 {
			continue
		}
		point := LatencyPoint{base, name, g.Now, g.TestURL, latest.Time, latest.Delay, m.latencySegment + "/" + selection.segment, !latest.Time.Before(selection.since), now}
		if !seen[key(point)] {
			m.disk.Latency = append(m.disk.Latency, point)
			seen[key(point)] = true
		}
	}
	active := map[string]bool{}
	for _, group := range groups {
		active[group.Group] = true
	}
	for name := range m.latencySelections {
		if !active[name] {
			delete(m.latencySelections, name)
		}
	}
	sort.Slice(groups, func(i, j int) bool { return groups[i].Group < groups[j].Group })
	m.latencyGroups = groups
	m.latencyError = ""
	m.prune(now)
}

func (m *Monitor) pollLatency() {
	m.mu.Lock()
	connected, base, generation := m.connected, m.base, m.generation
	m.mu.Unlock()
	if !connected {
		return
	}
	settings := m.settings()
	current, secret, err := resolve(settings)
	if err != nil || current != base {
		return
	}
	var proxies struct {
		Proxies map[string]latencyProxy `json:"proxies"`
	}
	var providers struct {
		Providers map[string]struct {
			Proxies []latencyProxy `json:"proxies"`
		} `json:"providers"`
	}
	err = get(base, secret, "/proxies", &proxies)
	if err == nil && proxies.Proxies == nil {
		err = errLatencyResponse
	}
	if err == nil {
		err = get(base, secret, "/providers/proxies", &providers)
	}
	if err != nil {
		m.mu.Lock()
		if m.base == base && m.generation == generation {
			m.latencyError = err.Error()
			m.latencyGroups = nil
			m.latencySegment = ""
			m.latencySelections = nil
			m.boundLatency = nil
		}
		m.mu.Unlock()
		return
	}
	m.ingestLatencyGeneration(base, proxies.Proxies, providers.Providers, time.Now(), generation)
}
