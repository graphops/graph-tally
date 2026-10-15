# GraphOps escrow manager fork

This branch starts at upstream `graph_tally_escrow_manager-v2.2.1` and adds support
for unauthenticated network subgraphs. The auth fix is a separate commit from
image packaging to make it easy to review or submit upstream.

`query_auth` may be omitted, `null`, or an empty string: no `Authorization` header
is sent. Non-empty tokens continue to send bearer authentication. This applies
both at startup and when rebuilding the client after transactions.

## Published image

- Branch: `graphops/2.2.1-optional-query-auth`
- Image: `harbor.mgmt.infra.graphops.xyz/infra/graph-tally-escrow-manager:2.2.1-graphops.1`
- Platform: `linux/amd64`
- Entrypoint: `/usr/local/bin/graph_tally_escrow_manager`
- Pass the JSON config path as the container argument, as with upstream v2.x.

`Cargo.lock` is tracked so image rebuilds use the tested dependency versions.
The escrow-manager Dockerfile runs unit and HTTP regression tests, Clippy, and
then the release build with the upstream Rust 1.91 toolchain and `--locked`.

```sh
podman build --platform linux/amd64 \
  -f docker/Dockerfile.graph_tally_escrow_manager \
  -t harbor.mgmt.infra.graphops.xyz/infra/graph-tally-escrow-manager:2.2.1-graphops.1 .
podman push --authfile "$HOME/.docker/config.json" \
  harbor.mgmt.infra.graphops.xyz/infra/graph-tally-escrow-manager:2.2.1-graphops.1
```

This is based on v2.2.1, rather than the deployed v2.0.0. Review upstream changes
when adopting it: v2.1.0 introduced the configurable funding margin (default 0.8),
and v2.2.0 added optional escrow reclamation (disabled by default). No deployment
configuration is changed by publishing this image.
