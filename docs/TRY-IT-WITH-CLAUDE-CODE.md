# Try ray-mcp end-to-end with Claude Code

A step-by-step guide to go from **nothing** to **asking Claude Code about a real
Ray cluster** through ray-mcp. No prior Ray or KubeRay knowledge needed.

You will:
1. spin up a throwaway Kubernetes cluster on your laptop (kind),
2. install the KubeRay operator (the thing that runs Ray on Kubernetes),
3. create a sample Ray cluster,
4. build ray-mcp and connect it to Claude Code,
5. ask Claude Code to list and inspect your Ray cluster.

Total time: ~20–30 min, most of it waiting for image downloads. Everything is
local and disposable — step 9 deletes it all.

---

## 0. Prerequisites

You said you already have these — quick checks so we fail early, not midway:

```sh
docker info        # Docker must be RUNNING (you'll see server info, not an error)
kubectl version --client   # any recent kubectl
go version         # need Go 1.26.3+ — if yours is lower, `go build` fails with a toolchain error
```

> **Apple Silicon Mac?** The Ray image (step 4) is published for `linux/amd64`
> only, so it runs under Docker Desktop emulation — slower and more memory-hungry.
> Give Docker Desktop **≥ 6 GB RAM** (Settings → Resources) or the worker pod can
> OOM / `CrashLoopBackOff`. On Intel Macs / Linux this isn't a concern.

Two more small tools. Install with Homebrew (macOS) or your package manager:

```sh
brew install kind helm     # kind = Kubernetes-in-Docker; helm = installs KubeRay
kind version
helm version
```

> **What is kind?** It runs a real Kubernetes cluster inside Docker containers on
> your laptop. Perfect for trying things out and throwing them away.

---

## 1. Clone and build ray-mcp

```sh
git clone https://github.com/risjai/ray-mcp.git
cd ray-mcp
go build -o ray-mcp ./cmd/ray-mcp
```

> This guide was tested end-to-end at commit **`42a4354`** (master). If master has
> moved and something doesn't match, `git checkout 42a4354` to get the exact
> version this guide describes.

This produces a `ray-mcp` binary in the current directory. Note its **absolute
path** — you'll need it for Claude Code:

```sh
echo "$(pwd)/ray-mcp"
```

> You can also install just the binary without cloning —
> `go install github.com/risjai/ray-mcp/cmd/ray-mcp@latest` (or `@v0.1.0`) — but
> this guide still clones, because step 4 uses the `examples/` manifest from the
> repo.

Keep that path handy (copy it somewhere).

---

## 2. Create a local Kubernetes cluster

```sh
kind create cluster --name ray-demo
```

This takes a minute or two the first time (it downloads a node image). When it's
done, confirm it works:

```sh
kubectl config use-context kind-ray-demo
kubectl get nodes          # should show one node, status Ready
```

> `kind create cluster` automatically points `kubectl` at the new cluster. The
> context is called `kind-ray-demo` — remember that name, ray-mcp will use it.

---

## 3. Install the KubeRay operator

KubeRay is the Kubernetes "operator" that knows how to run Ray. Install it with
Helm, pinned to the version ray-mcp is tested against (v1.6.1):

```sh
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm repo update
helm install kuberay-operator kuberay/kuberay-operator \
  --version 1.6.1 \
  --namespace kuberay-system --create-namespace --wait
```

Confirm it's running:

```sh
kubectl get pods -n kuberay-system
# NAME                                READY   STATUS    ...
# kuberay-operator-xxxxxxxxxx-xxxxx   1/1     Running   ...
```

---

## 4. Create a sample Ray cluster

The repo ships a laptop-sized sample. Apply it:

```sh
kubectl apply -f examples/raycluster-sample.yaml
```

Now wait for the Ray pods to start. **The first time, this pulls the Ray
container image (several GB) and can take 5–10 minutes** — that's normal, grab a
coffee:

```sh
kubectl get pods -l ray.io/cluster=ray-sample -w
```

Wait until you see a `head` pod and a `worker` pod both `Running` and `1/1`
ready, then press `Ctrl+C`. Check the cluster object:

```sh
kubectl get raycluster ray-sample
# NAME         DESIRED WORKERS   AVAILABLE WORKERS   ...   STATE   AGE
# ray-sample   1                 1                   ...   ready   2m
```

Once `STATE` is `ready` (or the workers show available), you have a real Ray
cluster running. 🎉

---

## 5. Try ray-mcp by hand (optional sanity check)

Before wiring Claude Code, confirm ray-mcp can see your cluster. ray-mcp speaks a
protocol called MCP over stdin/stdout; this one-liner sends it a couple of
messages and prints the reply:

```sh
{ printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cli","version":"0"}}}';
  sleep 0.4;
  printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}';
  printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"ray_cluster_list","arguments":{"namespace":"default"}}}';
  sleep 0.6;
} | ./ray-mcp --context kind-ray-demo --default-namespace default 2>/dev/null
```

You should see a JSON line listing `ray-sample` — something like (formatted here
for readability; it prints on one line):

```json
{"jsonrpc":"2.0","id":2,"result":{
  "content":[{"type":"text","text":"1 RayClusters in namespace \"default\" (showing all 1)"}],
  "structuredContent":{
    "clusters":[{"name":"ray-sample","namespace":"default","phase":"Ready",
                 "ready":1,"desired":1,"ageSeconds":122,
                 "health":"Ready; 1/1 workers ready"}],
    "count":1,"moreAvailable":false}}}
```

If you see your cluster with `"phase":"Ready"` — ray-mcp is talking to your live
cluster. 🎉 If you get a "cannot reach cluster" error, double-check
`kubectl get raycluster` works and that `--context kind-ray-demo` matches your
context (`kubectl config current-context`).

---

## 6. Connect ray-mcp to Claude Code

Register ray-mcp as an MCP server (use the absolute path from step 1):

```sh
claude mcp add --scope user ray-mcp /absolute/path/to/ray-mcp -- --context kind-ray-demo --default-namespace default
```

> The `--` separates Claude Code's own flags from ray-mcp's flags. Everything
> after `--` is passed to the ray-mcp binary.
>
> **Why `--scope user`?** Without it, `claude mcp add` defaults to `local` scope,
> which registers the server **only for the directory you ran the command in** — so
> starting Claude Code anywhere else won't show the tools (and the prompts in step 7
> silently do nothing). `--scope user` makes ray-mcp available in every directory.

Verify Claude Code sees it:

```sh
claude mcp list      # ray-mcp should appear
```

---

## 7. Ask Claude Code about your Ray cluster

Start Claude Code (`claude`) in any directory and try prompts like:

- *"Use the ray-mcp tools to list the Ray clusters."*
- *"Get the details of the ray-sample cluster."*
- *"Is ray-sample healthy? How many workers are ready?"*
- *"Call ray_capabilities and tell me what this server can do."*

Claude Code will call the ray-mcp tools (`ray_cluster_list`, `ray_cluster_get`,
`ray_capabilities`) and answer from the live cluster. If you run `/mcp` inside
Claude Code you'll see the three tools listed.

> Today ray-mcp exposes **read-only** RayCluster tools plus a capabilities tool.
> Creating/updating/deleting clusters, and Ray jobs/services, are on the way.

---

## 8. What you can and can't do yet

| You can | Not yet |
|---------|---------|
| List RayClusters (`ray_cluster_list`) | Create / update / delete clusters |
| Get one cluster's distilled status (`ray_cluster_get`) | Ray jobs (submit / logs / status) |
| Ask what the server supports (`ray_capabilities`) | Ray services |

This is an early build — the read path for RayClusters. Feedback on *this* slice
is exactly what's useful right now.

---

## 9. Clean up

When you're done, delete everything (it's all disposable):

```sh
kind delete cluster --name ray-demo
```

That removes the cluster, the operator, and the sample Ray cluster in one go.
Uninstall the Claude Code MCP entry if you like:

```sh
claude mcp remove --scope user ray-mcp
```

> Match the scope you added with. If you used `--scope user` in step 6, remove with
> `--scope user` (as above). If you skipped it (default `local`), run `claude mcp
> remove ray-mcp` from the **same directory** you added it in, or it reports
> `No MCP server found`.

---

## Troubleshooting

- **`docker info` errors** → Docker Desktop isn't running. Start it, retry.
- **Ray pods stuck `Pending` / `ContainerCreating`, or worker `CrashLoopBackOff` /
  OOMKilled** → still pulling the (big) image, or your Docker VM is low on memory —
  more likely on Apple Silicon where the amd64 image runs emulated. Give Docker
  Desktop **≥ 6 GB RAM**. `kubectl describe pod <name>` shows the reason at the bottom.
- **ray-mcp says "cannot reach cluster"** → the context is wrong or the cluster is
  down. Check `kubectl config current-context` (should be `kind-ray-demo`) and
  `kubectl get raycluster`. ray-mcp dials lazily, so fix the kubeconfig and just
  call the tool again — no restart needed.
- **Claude Code doesn't show the tools** → first, `claude mcp list` to confirm it's
  registered. If it's missing, the usual cause is **scope**: a default
  `claude mcp add` (no `--scope user`) only registers for the directory you ran it
  in, so the tools won't appear elsewhere — re-add with `--scope user` (step 6).
  Also make sure you used the **absolute** path to the binary. Run the step-5
  by-hand check to isolate whether it's ray-mcp or the Claude Code wiring.
- **`ray_capabilities` works but `ray_cluster_list` errors** → that's the
  lazy-dial behavior: capabilities needs no cluster, list does. The error means
  the cluster isn't reachable — see above.

---

Questions or weird behavior? Open an issue on the repo with the command you ran
and the output. Thanks for trying it!
