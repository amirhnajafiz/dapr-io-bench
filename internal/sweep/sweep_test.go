package sweep

import "testing"

func TestPointsOneAtATime(t *testing.T) {
	// payloads driven by one agent, agents driven at the baseline payload
	p := Plan{
		Mode:               ModeOneAtATime,
		Baseline:           Point{Agents: 8, PayloadBytes: 256},
		PayloadBytes:       []int{256, 1 << 30},
		PayloadSweepAgents: 1,
		Agents:             []int{1, 8, 1_000_000},
	}
	pts := p.Points()
	// baseline once; the 8-agent level is the baseline so it is not repeated,
	// and one agent at 256 B is shared by both sweeps so it is measured once
	if len(pts) != 1+2+1 {
		t.Fatalf("got %d points: %+v", len(pts), pts)
	}
	if pts[0].Dimension != DimBaseline {
		t.Errorf("first point is %s, want baseline", pts[0].Dimension)
	}
	for _, pt := range pts[1:] {
		switch pt.Dimension {
		case DimPayload:
			if pt.Agents != 1 {
				t.Errorf("payload point should have one agent: %+v", pt)
			}
		case DimAgents:
			if pt.PayloadBytes != 256 {
				t.Errorf("agents point should carry the baseline payload: %+v", pt)
			}
		case DimPayload + "," + DimAgents:
			if pt.Agents != 1 || pt.PayloadBytes != 256 {
				t.Errorf("shared point should be one agent at the baseline payload: %+v", pt)
			}
		default:
			t.Errorf("unexpected dimension %q", pt.Dimension)
		}
	}
}

func TestSharedLoadMeasuredOnceTaggedTwice(t *testing.T) {
	// one agent at 256 B is both the smallest payload level and the smallest
	// agent level
	p := Plan{
		Mode:               ModeOneAtATime,
		Baseline:           Point{Agents: 8, PayloadBytes: 256},
		PayloadBytes:       []int{256, 4096},
		PayloadSweepAgents: 1,
		Agents:             []int{1, 64},
	}
	pts := p.Points()
	if len(pts) != 1+2+1 {
		t.Fatalf("got %d points, want the shared load once: %+v", len(pts), pts)
	}
	for _, pt := range pts {
		if pt.Agents == 1 && pt.PayloadBytes == 256 && pt.Dimension != DimPayload+","+DimAgents {
			t.Errorf("shared point not tagged with both dimensions: %+v", pt)
		}
	}
}

func TestPointsGrid(t *testing.T) {
	p := Plan{
		Mode:         ModeGrid,
		Baseline:     Point{Agents: 8, PayloadBytes: 256},
		Agents:       []int{1, 64},
		PayloadBytes: []int{256, 4096},
	}
	if pts := p.Points(); len(pts) != 4 {
		t.Fatalf("got %d points, want 4", len(pts))
	}
}

func TestKeyspaceShrinksWithPayload(t *testing.T) {
	p := Plan{Keyspace: 1000, MaxDatasetBytes: 256 << 20}
	for payload, want := range map[int]int{256: 1000, 16 << 10: 1000, 1 << 20: 256, 128 << 20: 2, 1 << 30: 1} {
		if got := p.KeyspaceFor(payload); got != want {
			t.Errorf("keyspace for %d B: got %d, want %d", payload, got, want)
		}
	}
}

func TestTasksShrinkWithPayloadButCoverAgents(t *testing.T) {
	p := Plan{Tasks: 100_000, MaxTaskBytes: 8 << 30}
	cases := []struct {
		pt   Point
		want int
	}{
		{Point{Agents: 8, PayloadBytes: 256}, 100_000},
		{Point{Agents: 1, PayloadBytes: 1 << 20}, 8192},
		{Point{Agents: 1, PayloadBytes: 1 << 30}, 8},
		{Point{Agents: 1_000_000, PayloadBytes: 256}, 1_000_000},
	}
	for _, c := range cases {
		if got := p.TasksFor(c.pt); got != c.want {
			t.Errorf("tasks for %+v: got %d, want %d", c.pt, got, c.want)
		}
	}
}
