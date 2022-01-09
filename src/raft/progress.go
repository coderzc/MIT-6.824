package raft

type ProgressStateType int

const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)

var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

type progress struct {
	match, next int
	state       ProgressStateType
}

func (pr *progress) resetState(state ProgressStateType) {
	pr.state = state
}

func (pr *progress) becomeProbe() {
	pr.resetState(ProgressStateProbe)
	pr.next = pr.match + 1
}

func (pr *progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.next = pr.match + 1
}

func (pr *progress) optimisticUpdate(n int) { pr.next = n + 1 }

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
func (pr *progress) maybeDecrTo(rejected, last int) bool {
	if pr.state == ProgressStateReplicate {
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.match {
			return false
		}
		// directly decrease next to match + 1
		pr.next = pr.match + 1
		return true
	}

	// the rejection must be stale if "rejected" does not match next - 1
	if pr.next-1 != rejected {
		return false
	}

	if pr.next = min(rejected, last+1); pr.next < 1 {
		pr.next = 1
	}
	//pr.resume()
	return true
}

func (pr *progress) maybeUpdate(n int) bool {
	var updated bool
	if pr.match < n {
		pr.match = n
		updated = true
		//pr.resume()
	}
	if pr.next < n+1 {
		pr.next = n + 1
	}
	return updated
}
