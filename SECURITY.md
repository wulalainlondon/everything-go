# Security Policy

Please report vulnerabilities privately through GitHub Security Advisories for this repository. Do not include credentials, pairing tokens, service-account JSON, private keys, session transcripts, or private file contents in a public issue.

Supported releases are the latest published Go release. Security fixes may be backported when a prior release is still broadly deployed.

The bridge can execute local AI tools and expose local files selected by those tools. Pair only devices you control, keep tunnel credentials private, review high-risk permission prompts, and use `--root-dir` when a secondary instance should be restricted to one workspace.
