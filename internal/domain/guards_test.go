package domain

import (
	"errors"
	"strings"
	"testing"
)

// liveObj builds the minimal live-object shape the guards read: metadata.uid,
// metadata.resourceVersion, and (optionally) metadata.annotations. It mirrors the
// string-typed shape the unstructured converter produces under ClusterDetail.Raw.
func liveObj(uid, resourceVersion string, annotations map[string]string) MergedSpec {
	meta := map[string]any{
		"name":            "demo",
		"namespace":       "ray",
		"uid":             uid,
		"resourceVersion": resourceVersion,
	}
	if annotations != nil {
		a := map[string]any{}
		for k, v := range annotations {
			a[k] = v
		}
		meta["annotations"] = a
	}
	return MergedSpec{"metadata": meta}
}

// TestRequireConfirmMatchCommits is the core commit AC: a confirm equal to the
// recomputed fingerprint passes (nil error → the tool proceeds).
func TestRequireConfirmMatchCommits(t *testing.T) {
	t.Parallel()
	live := liveObj("uid-1", "100", nil)
	fp := Fingerprint(live, OpDelete)

	if err := RequireConfirm(live, OpDelete, fp); err != nil {
		t.Fatalf("matching confirm should commit, got error: %v", err)
	}
}

// TestRequireConfirmEmptyIsPreview asserts that an empty confirm is the preview
// half of the handshake: it returns a ConfirmRequiredError carrying the
// fingerprint the caller must echo back — NOT a generic failure.
func TestRequireConfirmEmptyIsPreview(t *testing.T) {
	t.Parallel()
	live := liveObj("uid-1", "100", nil)

	err := RequireConfirm(live, OpDelete, "")
	var req *ConfirmRequiredError
	if !errors.As(err, &req) {
		t.Fatalf("empty confirm should return ConfirmRequiredError, got %v", err)
	}
	if req.Fingerprint != Fingerprint(live, OpDelete) {
		t.Errorf("ConfirmRequiredError fingerprint = %q, want the recomputed fingerprint %q", req.Fingerprint, Fingerprint(live, OpDelete))
	}
	if req.Operation != OpDelete {
		t.Errorf("ConfirmRequiredError operation = %q, want %q", req.Operation, OpDelete)
	}
}

// TestRequireConfirmWrongValueRejects asserts a non-empty confirm that does not
// match is rejected as a ConfirmMismatchError.
func TestRequireConfirmWrongValueRejects(t *testing.T) {
	t.Parallel()
	live := liveObj("uid-1", "100", nil)

	err := RequireConfirm(live, OpDelete, "deadbeef")
	var mismatch *ConfirmMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("wrong confirm should return ConfirmMismatchError, got %v", err)
	}
}

// TestRequireConfirmStaleRejects is the stale-confirm AC: a fingerprint minted
// against one resourceVersion no longer matches once the resource changes (the
// resourceVersion is in the hash for content-mutating ops), so the now-stale
// confirm is rejected — the free TOCTOU / optimistic-concurrency guard.
func TestRequireConfirmStaleRejects(t *testing.T) {
	t.Parallel()
	preview := liveObj("uid-1", "100", nil)
	stale := Fingerprint(preview, OpScaleToZero)

	// The resource changed between preview and commit (e.g. an autoscaler tick or
	// any spec edit bumped resourceVersion).
	committed := liveObj("uid-1", "101", nil)

	err := RequireConfirm(committed, OpScaleToZero, stale)
	var mismatch *ConfirmMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("stale confirm should be rejected as ConfirmMismatchError, got %v", err)
	}
}

// TestConfirmMismatchDoesNotLeakFingerprint guards the TOCTOU intent: a mismatch
// (wrong OR stale) must NOT hand back the current fingerprint, or an agent could
// blindly retry without re-examining the changed resource. Only the explicit
// preview (empty confirm) yields a fingerprint.
func TestConfirmMismatchDoesNotLeakFingerprint(t *testing.T) {
	t.Parallel()
	live := liveObj("uid-1", "100", nil)

	err := RequireConfirm(live, OpScaleToZero, "wrong")
	current := Fingerprint(live, OpScaleToZero)
	if msg := err.Error(); strings.Contains(msg, current) {
		t.Errorf("mismatch error must not echo the current fingerprint %q (forces a re-preview); got %q", current, msg)
	}
}

// TestDeleteFingerprintIsIdentityOnly is the B1 decision: a delete fingerprint is
// hash(UID + op) with NO resourceVersion, so an autoscaler churning the
// resourceVersion between preview and commit cannot livelock the confirm.
func TestDeleteFingerprintIsIdentityOnly(t *testing.T) {
	t.Parallel()
	v100 := liveObj("uid-1", "100", nil)
	v101 := liveObj("uid-1", "101", nil)

	if Fingerprint(v100, OpDelete) != Fingerprint(v101, OpDelete) {
		t.Error("delete fingerprint must NOT depend on resourceVersion (B1: avoids autoscaler-churn livelock)")
	}
}

// TestScaleToZeroFingerprintIncludesResourceVersion is the B1 contrast: a
// content-mutating destructive op DOES bind resourceVersion, so a changed
// resource flips the fingerprint (the TOCTOU guard delete deliberately forgoes).
func TestScaleToZeroFingerprintIncludesResourceVersion(t *testing.T) {
	t.Parallel()
	v100 := liveObj("uid-1", "100", nil)
	v101 := liveObj("uid-1", "101", nil)

	if Fingerprint(v100, OpScaleToZero) == Fingerprint(v101, OpScaleToZero) {
		t.Error("scale-to-zero fingerprint must bind resourceVersion (free TOCTOU rejection)")
	}
}

// TestFingerprintBindsOperation asserts the operation is in the hash, so a
// confirm minted for one destructive op cannot authorize a different op on the
// same object (no cross-op replay).
func TestFingerprintBindsOperation(t *testing.T) {
	t.Parallel()
	live := liveObj("uid-1", "100", nil)

	if Fingerprint(live, OpDelete) == Fingerprint(live, OpScaleToZero) {
		t.Error("fingerprints for different operations on the same object must differ")
	}
	// A delete-confirm must not satisfy a scale-to-zero.
	deleteFP := Fingerprint(live, OpDelete)
	if err := RequireConfirm(live, OpScaleToZero, deleteFP); err == nil {
		t.Error("a delete fingerprint must not authorize a scale-to-zero")
	}
}

// TestFingerprintBindsIdentity asserts two distinct objects (different UID) never
// share a fingerprint, so a confirm for one resource cannot delete another.
func TestFingerprintBindsIdentity(t *testing.T) {
	t.Parallel()
	a := liveObj("uid-a", "100", nil)
	b := liveObj("uid-b", "100", nil)

	if Fingerprint(a, OpDelete) == Fingerprint(b, OpDelete) {
		t.Error("fingerprints for different resources (UIDs) must differ")
	}
}

// TestIsProtected asserts only the literal "true" annotation value protects.
func TestIsProtected(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		ann  map[string]string
		want bool
	}{
		{"protected", map[string]string{ProtectedAnnotation: "true"}, true},
		{"false value", map[string]string{ProtectedAnnotation: "false"}, false},
		{"other value", map[string]string{ProtectedAnnotation: "yes"}, false},
		{"absent", map[string]string{"other": "true"}, false},
		{"no annotations", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsProtected(liveObj("uid-1", "100", tc.ann)); got != tc.want {
				t.Errorf("IsProtected(%v) = %v, want %v", tc.ann, got, tc.want)
			}
		})
	}
}

// TestGuardProtectedChangeRefusesRemovalWithoutConfirm is the AC2 core: a write
// that strips the protected annotation is refused unless a valid fingerprint is
// presented — otherwise the guard is decorative (strip-then-delete).
func TestGuardProtectedChangeRefusesRemovalWithoutConfirm(t *testing.T) {
	t.Parallel()
	before := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})
	after := liveObj("uid-1", "100", nil) // the annotation removed

	err := GuardProtectedChange(before, after, "")
	var req *ConfirmRequiredError
	if !errors.As(err, &req) {
		t.Fatalf("removing protection without confirm must be refused (ConfirmRequiredError), got %v", err)
	}
	if req.Operation != OpUnprotect {
		t.Errorf("protected-removal confirm operation = %q, want %q", req.Operation, OpUnprotect)
	}
}

// TestGuardProtectedChangeRefusesAlterWithoutConfirm asserts changing the value
// away from "true" (not just deleting the key) is also a weakening and refused.
func TestGuardProtectedChangeRefusesAlterWithoutConfirm(t *testing.T) {
	t.Parallel()
	before := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})
	after := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "false"})

	if err := GuardProtectedChange(before, after, ""); err == nil {
		t.Error("changing protection from true to false without confirm must be refused")
	}
}

// TestGuardProtectedChangeAllowsRemovalWithConfirm asserts a valid unprotect
// fingerprint lets the protection-stripping write through.
func TestGuardProtectedChangeAllowsRemovalWithConfirm(t *testing.T) {
	t.Parallel()
	before := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})
	after := liveObj("uid-1", "100", nil)

	fp := Fingerprint(before, OpUnprotect)
	if err := GuardProtectedChange(before, after, fp); err != nil {
		t.Errorf("a valid unprotect confirm should allow the change, got %v", err)
	}
}

// TestGuardProtectedChangeAllowsAddingProtection asserts adding protection
// (unprotected → protected) needs no confirm — you can always strengthen a guard.
func TestGuardProtectedChangeAllowsAddingProtection(t *testing.T) {
	t.Parallel()
	before := liveObj("uid-1", "100", nil)
	after := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})

	if err := GuardProtectedChange(before, after, ""); err != nil {
		t.Errorf("adding protection should not require a confirm, got %v", err)
	}
}

// TestGuardProtectedChangeNoopWhenUnchanged asserts a write that leaves the
// protected annotation in place is not gated (protected → protected).
func TestGuardProtectedChangeNoopWhenUnchanged(t *testing.T) {
	t.Parallel()
	before := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})
	after := liveObj("uid-1", "100", map[string]string{ProtectedAnnotation: "true"})

	if err := GuardProtectedChange(before, after, ""); err != nil {
		t.Errorf("an unchanged protected annotation should not require a confirm, got %v", err)
	}
}
