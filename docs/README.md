# Documentation Map

This directory keeps detailed documentation out of the root README while making
it cheap for people and AI agents to find the right source of truth.

| Document | Read this when | Ownership |
| --- | --- | --- |
| [../README.md](../README.md) | You need a quick start or product overview. | First-run path, links, short examples only. |
| [cli-reference.md](cli-reference.md) | You need command names, flags, defaults, or usage examples. | CLI surface and user-facing defaults. |
| [how-it-works.md](how-it-works.md) | You need setup internals, generated files, config rewrites, or key delivery details. | Under-the-hood behavior and file layout. |
| [mcp.md](mcp.md) | You need MCP server workflow, client setup, or transaction argument rules. | MCP behavior and MCP-specific examples. |
| [hooks-and-upgrades.md](hooks-and-upgrades.md) | You need protocol staging, hook timing, or upgrade behavior. | Hook lifecycle and upgrade operations. |
| [diagnostics.md](diagnostics.md) | You need diagnostics websocket or HTTP proxy usage. | Diagnostics proxy behavior and examples. |

## Agent Ingestion Guide

Start with `AGENTS.md` and this file. Then read only the document that matches
the behavior you are changing. Use `rg` to find exact commands, flags, or
section headings before opening long docs.

Do not bulk-read every file in `docs/` unless the change is cross-cutting. For
example, an MCP transaction argument change usually needs `mcp.md` and maybe
`cli-reference.md`, but not `diagnostics.md`.

## Consistency Rules

When user-facing CLI behavior changes, update the generated behavior in the
code, the relevant detailed doc, and the README if the change affects the quick
start or common workflows.

When adding a new detailed doc, link it from this file and from the README
Documentation table. Keep detailed reference prose in one canonical doc and
link to it instead of copying long sections into multiple files.

Verify command names, flags, and defaults with `cargo run --quiet -- ... --help`
before editing docs that mention them.
