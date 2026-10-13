CI runs lint, format and tests on every push and pull request.

`GITHUB_TOKEN` is provided automatically by Actions — this repository needs no
personal access token and no deploy key for CI. If a workflow ever appears to
need one, that is a signal the workflow is doing something it should not.
