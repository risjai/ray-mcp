package domain

import (
	"context"
	"errors"
	"testing"
)

// --- classifyServiceUpdate table-driven tests --------------------------------

func TestClassifyServiceUpdate(t *testing.T) {
	t.Parallel()

	// baseRCC is a minimal rayClusterConfig with a head group and one worker group.
	baseRCC := func() map[string]any {
		return map[string]any{
			"rayVersion": "2.9.0",
			"headGroupSpec": map[string]any{
				"template": map[string]any{
					"spec": map[string]any{
						"containers": []any{
							map[string]any{"name": "ray-head", "image": "rayproject/ray:2.9.0"},
						},
					},
				},
			},
			"workerGroupSpecs": []any{
				map[string]any{
					"groupName":   "workers",
					"replicas":    int64(2),
					"minReplicas": int64(0),
					"maxReplicas": int64(5),
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0"},
							},
						},
					},
				},
			},
		}
	}

	tests := []struct {
		name string
		live map[string]any
		sub  map[string]any
		want string
	}{
		{
			name: "serve-config-only change → in-place",
			live: map[string]any{
				"serveConfigV2":    "old-config",
				"rayClusterConfig": baseRCC(),
			},
			sub: map[string]any{
				"serveConfigV2":    "new-config",
				"rayClusterConfig": baseRCC(),
			},
			want: "in-place",
		},
		{
			name: "cluster-spec change with upgradeStrategy NewCluster → zero-downtime-swap",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				rcc["rayVersion"] = "2.10.0" // changed!
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			want: "zero-downtime-swap",
		},
		{
			name: "cluster-spec change with upgradeStrategy None → hedged message",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "None"},
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				rcc["rayVersion"] = "2.10.0"
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "None"},
				}
			}(),
			want: "zero-downtime-swap, OR no-op if the operator has ENABLE_ZERO_DOWNTIME=false (upgradeStrategy.type=None disables swap; the tool cannot see the operator env)",
		},
		{
			name: "cluster-spec change with upgradeStrategy unset → hedged (default-enabled)",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				rcc["rayVersion"] = "2.10.0"
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			want: "zero-downtime-swap (predicted; operator default enables it, but upgradeStrategy.type is unset — if ENABLE_ZERO_DOWNTIME=false in the operator env, this change is a no-op)",
		},
		{
			name: "replicas-only change → scale (no swap)",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				// Change only replicas (excluded from hash).
				wg := rcc["workerGroupSpecs"].([]any)[0].(map[string]any)
				wg["replicas"] = int64(4)
				wg["maxReplicas"] = int64(10)
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			want: "scale (no swap)",
		},
		{
			name: "tolerations-only change → scale (no swap)",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				// Add tolerations to a worker group (excluded from hash).
				wg := rcc["workerGroupSpecs"].([]any)[0].(map[string]any)
				tmpl := wg["template"].(map[string]any)
				podSpec := tmpl["spec"].(map[string]any)
				podSpec["tolerations"] = []any{map[string]any{"key": "gpu", "effect": "NoSchedule"}}
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
				}
			}(),
			want: "scale (no swap)",
		},
		{
			// KubeRay v1.6.1: APPENDING a worker group (every existing group's
			// hash-affecting fields unchanged) updates the EXISTING cluster in place —
			// shouldPrepareNewRayCluster truncates the submitted groups to the live
			// count, finds the hash equal, and returns false BEFORE consulting
			// zero-downtime upgrade. So this is in-place even with upgradeStrategy set.
			name: "appended worker group → in-place cluster update (no swap)",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				// Append a new worker group, leaving the existing group untouched.
				groups := rcc["workerGroupSpecs"].([]any)
				rcc["workerGroupSpecs"] = append(groups, map[string]any{
					"groupName":   "gpu-workers",
					"replicas":    int64(1),
					"minReplicas": int64(0),
					"maxReplicas": int64(2),
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0-gpu"},
							},
						},
					},
				})
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			want: "in-place cluster update (worker group added, no swap)",
		},
		{
			// Contrast: CHANGING an existing worker group's template (not a pure
			// append) is a genuine cluster-spec change → swap. This guards the
			// partial-match tier from over-matching (it must only fire on a strict
			// trailing append, never when a pre-existing group's hash changed).
			name: "changed existing worker group → zero-downtime-swap",
			live: func() map[string]any {
				rcc := baseRCC()
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			sub: func() map[string]any {
				rcc := baseRCC()
				// Mutate the EXISTING worker group's image, then append a new group.
				groups := rcc["workerGroupSpecs"].([]any)
				wg0 := groups[0].(map[string]any)
				tmpl := wg0["template"].(map[string]any)
				podSpec := tmpl["spec"].(map[string]any)
				podSpec["containers"] = []any{
					map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.10.0"},
				}
				rcc["workerGroupSpecs"] = append(groups, map[string]any{
					"groupName": "gpu-workers",
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0-gpu"},
							},
						},
					},
				})
				return map[string]any{
					"serveConfigV2":    "cfg",
					"rayClusterConfig": rcc,
					"upgradeStrategy":  map[string]any{"type": "NewCluster"},
				}
			}(),
			want: "zero-downtime-swap",
		},
		{
			name: "no change → no change detected",
			live: map[string]any{
				"serveConfigV2":    "cfg",
				"rayClusterConfig": baseRCC(),
			},
			sub: map[string]any{
				"serveConfigV2":    "cfg",
				"rayClusterConfig": baseRCC(),
			},
			want: "no change detected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := classifyServiceUpdate(tt.live, tt.sub)
			if got != tt.want {
				t.Errorf("classifyServiceUpdate = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- ServiceWriteService.Deploy tests ----------------------------------------

// fakeServiceBaseBuilder records the params it was handed and returns a canned base.
type fakeServiceBaseBuilder struct {
	got  ServiceDeployParams
	base MergedSpec
	err  error
}

func (f *fakeServiceBaseBuilder) BuildServiceBase(p ServiceDeployParams) (MergedSpec, error) {
	f.got = p
	return f.base, f.err
}

var _ ServiceBaseBuilder = (*fakeServiceBaseBuilder)(nil)

// fakeServiceGetter returns a canned ServiceDetail for update tests.
type fakeServiceGetter struct {
	detail ServiceDetail
	err    error
}

func (f *fakeServiceGetter) GetService(_ context.Context, _, _ string) (ServiceDetail, error) {
	if f.err != nil {
		return ServiceDetail{}, f.err
	}
	return f.detail, nil
}

var _ ServiceGetter = (*fakeServiceGetter)(nil)

// serviceBaseFor builds the curated base a real builder would produce for the given
// identity.
func serviceBaseFor(namespace, name string) MergedSpec {
	return MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayService",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"spec": map[string]any{
			"rayClusterConfig": map[string]any{"rayVersion": "2.9.0"},
		},
	}
}

func newServiceWriteService(base ServiceBaseBuilder, get ServiceGetter, applier Applier, defaultNS string) (*ServiceWriteService, *recordingSink) {
	sink := &recordingSink{}
	svc := NewServiceWriteService(base, get, NewApplyService(applier, sink), defaultNS)
	return svc, sink
}

func TestServiceDeployBuildsMergesAndApplies(t *testing.T) {
	t.Parallel()
	base := &fakeServiceBaseBuilder{base: serviceBaseFor("ray", "svc1")}
	applier := &fakeApplier{dryRunObj: serviceBaseFor("ray", "svc1"), applyObj: serviceBaseFor("ray", "svc1")}
	svc, sink := newServiceWriteService(base, &fakeServiceGetter{}, applier, "ray")

	res, err := svc.Deploy(context.Background(), ServiceDeployParams{
		Name: "svc1", RayVersion: "2.9.0",
	})
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if res.DryRun {
		t.Error("result DryRun = true, want false for a committed deploy")
	}
	if res.Name != "svc1" || res.Namespace != "ray" {
		t.Errorf("result identity = %s/%s, want ray/svc1", res.Namespace, res.Name)
	}
	// Namespace default was applied before building the base.
	if base.got.Namespace != "ray" {
		t.Errorf("base builder saw namespace %q, want the resolved default %q", base.got.Namespace, "ray")
	}
	// Two applier calls: dry-run then commit.
	if len(applier.calls) != 2 || !applier.calls[0].dryRun || applier.calls[1].dryRun {
		t.Fatalf("applier calls = %+v, want [dry-run, commit]", applier.calls)
	}
	// The applied spec kind is RayService.
	if applier.calls[1].kind != KindRayService {
		t.Errorf("applied kind = %s, want RayService", applier.calls[1].kind)
	}
	// One audit record.
	if len(sink.records) != 1 || sink.records[0].Tool != "ray_service_deploy" {
		t.Fatalf("audit records = %+v, want one tagged ray_service_deploy", sink.records)
	}
}

func TestServiceDeployDryRunDoesNotCommit(t *testing.T) {
	t.Parallel()
	base := &fakeServiceBaseBuilder{base: serviceBaseFor("ray", "svc1")}
	applier := &fakeApplier{dryRunObj: serviceBaseFor("ray", "svc1")}
	svc, _ := newServiceWriteService(base, &fakeServiceGetter{}, applier, "ray")

	res, err := svc.Deploy(context.Background(), ServiceDeployParams{Name: "svc1", DryRun: true})
	if err != nil {
		t.Fatalf("Deploy(dryRun): %v", err)
	}
	if !res.DryRun {
		t.Error("result DryRun = false, want true")
	}
	if len(applier.calls) != 1 || !applier.calls[0].dryRun {
		t.Fatalf("applier calls = %+v, want exactly one dry-run", applier.calls)
	}
}

func TestServiceDeployRequiresName(t *testing.T) {
	t.Parallel()
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, &fakeServiceGetter{}, &fakeApplier{}, "ray")
	if _, err := svc.Deploy(context.Background(), ServiceDeployParams{Name: ""}); err == nil {
		t.Fatal("Deploy with empty name returned nil error, want a validation error")
	}
}

func TestServiceDeployDefaultsNamespace(t *testing.T) {
	t.Parallel()
	base := &fakeServiceBaseBuilder{base: serviceBaseFor("prod", "svc1")}
	applier := &fakeApplier{dryRunObj: serviceBaseFor("prod", "svc1"), applyObj: serviceBaseFor("prod", "svc1")}
	svc, _ := newServiceWriteService(base, &fakeServiceGetter{}, applier, "prod")

	if _, err := svc.Deploy(context.Background(), ServiceDeployParams{Name: "svc1"}); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if base.got.Namespace != "prod" {
		t.Errorf("base builder saw namespace %q, want the default %q", base.got.Namespace, "prod")
	}
}

// --- ServiceWriteService.Update tests ----------------------------------------

// liveService builds a ServiceDetail with a known spec for update tests.
func liveService(namespace, name string) ServiceDetail {
	raw := MergedSpec{
		"apiVersion": "ray.io/v1",
		"kind":       "RayService",
		"metadata": map[string]any{
			"name":            name,
			"namespace":       namespace,
			"resourceVersion": "999",
			"managedFields":   []any{map[string]any{"manager": "ray-mcp"}},
		},
		"spec": map[string]any{
			"serveConfigV2": "old-config",
			"rayClusterConfig": map[string]any{
				"rayVersion": "2.9.0",
				"headGroupSpec": map[string]any{
					"template": map[string]any{
						"spec": map[string]any{
							"containers": []any{
								map[string]any{"name": "ray-head", "image": "rayproject/ray:2.9.0"},
							},
						},
					},
				},
				"workerGroupSpecs": []any{
					map[string]any{
						"groupName":   "workers",
						"replicas":    int64(2),
						"minReplicas": int64(0),
						"maxReplicas": int64(5),
						"template": map[string]any{
							"spec": map[string]any{
								"containers": []any{
									map[string]any{"name": "ray-worker", "image": "rayproject/ray:2.9.0"},
								},
							},
						},
					},
				},
			},
		},
		"status": map[string]any{"serviceStatus": "Running"},
	}
	return ServiceDetail{
		ServiceSummary: ServiceSummary{Name: name, Namespace: namespace},
		Raw:            raw,
	}
}

func TestServiceUpdateRejectsEmpty(t *testing.T) {
	t.Parallel()
	getter := &fakeServiceGetter{detail: liveService("ray", "svc1")}
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, getter, &fakeApplier{}, "ray")

	_, err := svc.Update(context.Background(), ServiceUpdateParams{Name: "svc1"})
	if err == nil {
		t.Fatal("Update with no changes returned nil error, want rejection")
	}
}

func TestServiceUpdateRequiresName(t *testing.T) {
	t.Parallel()
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, &fakeServiceGetter{}, &fakeApplier{}, "ray")
	newCfg := "new"
	_, err := svc.Update(context.Background(), ServiceUpdateParams{ServeConfigV2: &newCfg})
	if err == nil {
		t.Fatal("Update with empty name returned nil error, want a validation error")
	}
}

func TestServiceUpdateServeConfigReturnsInPlace(t *testing.T) {
	t.Parallel()
	getter := &fakeServiceGetter{detail: liveService("ray", "svc1")}
	applier := &fakeApplier{
		dryRunObj: serviceBaseFor("ray", "svc1"),
		applyObj:  serviceBaseFor("ray", "svc1"),
	}
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, getter, applier, "ray")

	newCfg := "new-config"
	res, err := svc.Update(context.Background(), ServiceUpdateParams{
		Name:          "svc1",
		ServeConfigV2: &newCfg,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.PredictedPath != "in-place" {
		t.Errorf("PredictedPath = %q, want in-place", res.PredictedPath)
	}
}

func TestServiceUpdateImageReturnsSwap(t *testing.T) {
	t.Parallel()
	detail := liveService("ray", "svc1")
	// Add upgradeStrategy to force a clean swap prediction.
	spec := detail.Raw["spec"].(map[string]any)
	spec["upgradeStrategy"] = map[string]any{"type": "NewCluster"}
	getter := &fakeServiceGetter{detail: detail}
	applier := &fakeApplier{
		dryRunObj: serviceBaseFor("ray", "svc1"),
		applyObj:  serviceBaseFor("ray", "svc1"),
	}
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, getter, applier, "ray")

	res, err := svc.Update(context.Background(), ServiceUpdateParams{
		Name:  "svc1",
		Image: "rayproject/ray:2.10.0",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.PredictedPath != "zero-downtime-swap" {
		t.Errorf("PredictedPath = %q, want zero-downtime-swap", res.PredictedPath)
	}
}

func TestServiceUpdateDefaultsNamespace(t *testing.T) {
	t.Parallel()
	getter := &fakeServiceGetter{detail: liveService("prod", "svc1")}
	applier := &fakeApplier{
		dryRunObj: serviceBaseFor("prod", "svc1"),
		applyObj:  serviceBaseFor("prod", "svc1"),
	}
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, getter, applier, "prod")

	newCfg := "x"
	res, err := svc.Update(context.Background(), ServiceUpdateParams{
		Name:          "svc1",
		ServeConfigV2: &newCfg,
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res.Namespace != "prod" {
		t.Errorf("result Namespace = %q, want prod", res.Namespace)
	}
}

func TestServiceUpdateNotFoundPropagates(t *testing.T) {
	t.Parallel()
	getter := &fakeServiceGetter{err: &NotFoundError{Kind: KindRayService, Namespace: "ray", Name: "gone"}}
	svc, _ := newServiceWriteService(&fakeServiceBaseBuilder{}, getter, &fakeApplier{}, "ray")

	newCfg := "x"
	_, err := svc.Update(context.Background(), ServiceUpdateParams{Name: "gone", ServeConfigV2: &newCfg})
	var nf *NotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Update error = %v, want *NotFoundError", err)
	}
}
