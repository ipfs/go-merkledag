package merkledag_test

import (
	"context"
	"sync"
	"testing"
	"time"

	cid "github.com/ipfs/go-cid"
	. "github.com/ipfs/go-merkledag"
	dstest "github.com/ipfs/go-merkledag/test"
)

func TestProgressIndicator(t *testing.T) {
	testProgressIndicator(t, 5)
}

func TestProgressIndicatorNoChildren(t *testing.T) {
	testProgressIndicator(t, 0)
}

func testProgressIndicator(t *testing.T, depth int) {
	ds := dstest.Mock()

	top, numChildren := mkDag(ds, depth)

	progressTracker := NewProgressTracker()
	ctx := WithProgressTracker(context.Background(), progressTracker)

	err := FetchGraph(ctx, top, ds)
	if err != nil {
		t.Fatal(err)
	}

	if progressTracker.TotalToPin() != numChildren+1 {
		t.Errorf("wrong number of children reported in progress indicator, expected %d, got %d",
			numChildren+1, progressTracker.TotalToPin())
	}

	plannedToPin := progressTracker.PopPlannedToPin()
	if len(plannedToPin) != progressTracker.TotalToPin() {
		t.Errorf("wrong number of children reported in progress indicator (total does not match concrete cids count), expected %d, got %d",
			len(plannedToPin), progressTracker.TotalToPin())
	}
}

func TestProgressIndicatorFlow(t *testing.T) {
	progressTracker := NewProgressTracker()
	ctx := WithProgressTracker(context.Background(), progressTracker)

	ongoingCids := make(chan cid.Cid)
	actualPinCids := make([]cid.Cid, 0)
	registeredToPinCids := make([]cid.Cid, 0)

	go func(ctx context.Context) {
		ticker := time.NewTicker(5 * time.Millisecond)
		defer func() {
			close(ongoingCids)
			ticker.Stop()
		}()

		progressTracker := GetProgressTracker(ctx)
		upTo := time.After(1 * time.Second)

		for {
			select {
			case <-ticker.C:
				node := mkProtoNode()
				ongoingCids <- node.Cid()
				progressTracker.PlanToPin(node.Cid())
			case <-upTo:
				return
			}

		}
	}(ctx)

	cCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func(ctx context.Context) {
		defer func() {
			registeredToPinCids = append(
				registeredToPinCids,
				progressTracker.PopPlannedToPin()...)
			wg.Done()
		}()

		progressTracker := GetProgressTracker(ctx)
		ticker := time.NewTicker(3 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				registeredToPinCids = append(
					registeredToPinCids,
					progressTracker.PopPlannedToPin()...)
			case <-ctx.Done():
				return
			}
		}
	}(cCtx)

	for cid := range ongoingCids {
		actualPinCids = append(actualPinCids, cid)
	}
	cancel()
	wg.Wait()

	if len(actualPinCids) != len(registeredToPinCids) {
		t.Errorf("actual and registered pins mismatch: %d vs %d",
			len(actualPinCids), len(registeredToPinCids))
	}

	for i := 0; i < len(actualPinCids)-1; i++ {
		if actualPinCids[i] != registeredToPinCids[i] {
			t.Errorf("actual and registered pins mismatch at %d: %v vs %v",
				i, actualPinCids[i], registeredToPinCids[i])
		}
	}
}
