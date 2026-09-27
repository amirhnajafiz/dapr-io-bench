package sweep

import "testing"

func TestPointsOneAtATime(t *testing.T) {
	p := Plan{
		Mode:          ModeOneAtATime,
		Baseline:      Point{Rate: 200, PayloadBytes: 256, Concurrency: 8},
		Rates:         []int{50, 200, 1000},
		PayloadBytes:  []int{256, 4096},
		Concurrencies: []int{1, 8, 64},
	}
	pts := p.Points()
	// baseline once, plus every non-baseline level of each list
	if len(pts) != 1+2+1+2 {
		t.Fatalf("got %d points: %+v", len(pts), pts)
	}
	if pts[0].Dimension != DimBaseline {
		t.Errorf("first point is %s, want baseline", pts[0].Dimension)
	}
	for _, pt := range pts[1:] {
		switch pt.Dimension {
		case DimRate:
			if pt.PayloadBytes != 256 || pt.Concurrency != 8 {
				t.Errorf("rate point moved other dims: %+v", pt)
			}
		case DimPayload:
			if pt.Rate != 200 || pt.Concurrency != 8 {
				t.Errorf("payload point moved other dims: %+v", pt)
			}
		case DimConcurrency:
			if pt.Rate != 200 || pt.PayloadBytes != 256 {
				t.Errorf("concurrency point moved other dims: %+v", pt)
			}
		default:
			t.Errorf("unexpected dimension %q", pt.Dimension)
		}
	}
}

func TestPointsGrid(t *testing.T) {
	p := Plan{
		Mode:         ModeGrid,
		Baseline:     Point{Rate: 200, PayloadBytes: 256, Concurrency: 8},
		Rates:        []int{50, 1000},
		PayloadBytes: []int{256, 4096},
		// empty list: held at baseline
	}
	pts := p.Points()
	if len(pts) != 4 {
		t.Fatalf("got %d points, want 4", len(pts))
	}
	for _, pt := range pts {
		if pt.Concurrency != 8 || pt.Dimension != DimGrid {
			t.Errorf("bad grid point: %+v", pt)
		}
	}
}
