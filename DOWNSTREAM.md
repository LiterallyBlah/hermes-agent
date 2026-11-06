# Maintained downstream: Hermes Agent

This fork carries narrowly scoped integration changes while continuing to incorporate upstream Hermes development. Prefer supported configuration/plugins and remove downstream patches when upstream genuinely replaces them.

- Owned repository: https://github.com/LiterallyBlah/hermes-agent
- Maintained branch: `blahsec-integration`
- Vendor repository (`upstream`): https://github.com/NousResearch/hermes-agent
- Initial vendor baseline: `c314285f077e91937c995d419b9fa40af9eb4981`.
- `origin` is the owned fork; `upstream` is the vendor. Verify both URLs before updates/pushes.

## Update contract

**An update means merging selected upstream changes into our maintained code, validating the combination, and promoting that tested candidate. It does not mean resetting to a stock release or synchronising our custom branch to the vendor.**

Do not use an ordinary `hermes update`, release-channel switch, fork force-sync, reset, or reinstall as a substitute for this procedure. The vendor updater has useful preservation/recovery mechanisms, but preserving an edit or a rescue ref is not proof that the running release still contains a working downstream feature. Existing source checkouts, package-manager environments and running gateways may point at different revisions; discover the installation actually executing the work.

1. Inspect the live launcher, installation-bound source, active services/gateway authority, branch/remotes and working tree. Preserve unrelated edits and feature branches. Never automatically stage, stash, reset or commit someone else's work. Use an isolated candidate worktree.
2. Fetch the owned fork and vendor. Read the selected upstream release's changes. Create an `update/<release>` candidate from `origin/blahsec-integration`, then merge the exact chosen upstream tag/commit. Keep the maintained patch set rather than replacing it with one side of a conflict.
3. Review the patch ledger and existing native extension points. When upstream provides equivalent behaviour, remove our replacement code in a separate focused change, preserving compatibility and behavioural tests. A similarly named setting or feature is not enough.
4. Read root/area `AGENTS.md`, `CONTRIBUTING.md` and the current package-manager instructions. Provision isolated development/test environments through Hermes PM; do not mutate live environments with pip/uv. Run the canonical `scripts/run_tests.sh` for the changed contract and relevant regression surface, `python scripts/check` for blocking lint, and the release/full-suite checks required by the checkout. Use isolated homes/runtime directories, no real credentials or live user state. Do not bypass HomeIOGuard or weaken tests to obtain a pass.
5. Build/package the combined candidate through the supported package manager. Record the tested commit, dependency/build identity and check results. Account for cross-version gateway protocol/client compatibility and all active frontends before activation.
6. Back up affected configuration, profile/session state and supported install metadata separately from Git, preserving file permissions and secret isolation. Reuse still-applicable evidence rather than rerunning unrelated experiments. Never silently omit uncommitted fixes that the live installation currently depends on; either preserve them during a selective deployment or explicitly integrate them under their own authorised work.
7. Review, promote and push the tested candidate to `blahsec-integration`, then independently read back its remote commit. Activate through the actual installation's supported mechanism. Restart/reload only the relevant authority and clients, respecting active work. Existing processes may retain imported old code.
8. Verify real CLI/ACP/gateway behaviour and the downstream acceptance checks after activation. Retain a compatible rollback code/environment/state combination. Roll back deliberately on failure, not by deleting useful partial work.

Example source preparation:

```sh
git remote -v
git fetch origin
git fetch upstream --tags
git worktree add -b update/RELEASE ../hermes-update-RELEASE origin/blahsec-integration
cd ../hermes-update-RELEASE
git merge --no-ff UPSTREAM_REF
# Resolve, inspect, provision, test and build before promoting/deploying.
```

No scheduled merges, unattended deployment, publication upstream, new infrastructure or spend is implied. Update when requested; batch maintenance into a tested candidate.

## Integration ledger

The bounded external-run integration is developed on `paperclip-run-scope` and merged here only after verification. Keep the implementation commits and exact acceptance evidence recorded when it lands. This is a generic execution capability, not Paperclip-specific behaviour in Hermes core.

Required contract:

- An explicitly opted-in external run can supply named temporary integration environment values, including credential-shaped values, through the existing authenticated local gateway protocol into only that run's tool scope.
- Values remain transient, bounded and in memory. They must not enter argv, model prompts, saved histories/DB, session snapshots, config values, ordinary logs or files. Keep the ordinary public client-environment filter unchanged.
- Isolate profiles and sessions, refresh safely for a later run reusing a conversation, refuse unsafe replacement during an active run, and clear the overlay after completion/cancellation. Do not inherit the launching client's entire environment.
- Existing terminal and code-execution paths can use the selected overlay without exposing it to unrelated sessions. Permanent owner credentials are not a replacement for per-run identity.
- Ordinary interactive/one-shot clients retain their established detach semantics. An explicitly bounded externally owned run must be interrupted through the gateway's native generation-fenced cancellation when its controller ends; killing a client process alone is not verified cancellation.
- Tests cover the real gateway/executor boundary, isolation, reuse/cleanup, non-persistence/redaction and cancellation. A captured CLI invocation or passing model hello does not prove this contract.

Retire the downstream implementation only when an upstream release satisfies those behaviours and the matching controller/adapter still passes its real-run tests. Delete obsolete code rather than leaving two implementations active. Preserve behaviour-contract tests while adapting them to the upstream interface.

## State and other local work

This source fork must not contain profiles, provider tokens, board credentials, runtime overlays, user memory, session databases, logs or private deployment details. Profiles are state separation, not separate installed code; a shared-gateway code update can affect multiple frontends. Native extension/plugin compatibility and profile boundaries remain required.

The original deployment checkout can contain unrelated local fixes. Its presence in this fork's remote configuration does not mean those fixes have been reviewed, committed or published. Do not absorb them into the external-run patch merely to obtain a clean working tree.

Host-specific source/launcher mappings and activation cautions belong in the operator's private Hermes skill reference, not in this public repository.
