# redis.el Development Guide

Elisp best practices for the native Redis RESP client.  This repository is the
standalone protocol package used by caller applications such as Clutch.

Before changing this repository, read this file.  Before changing a caller
adapter, read that caller's own guide as well.

## Core Principles

- Keep this package protocol-only.  Do not add UI, query-console, grid
  rendering, object browsers, or Clutch-specific behavior.
- Keep the first supported surface intentionally small: RESP2, one TCP
  connection, one command/response at a time, AUTH, SELECT, and structured Redis
  errors.
- Add abstractions only when they simplify current code or protect a real public
  boundary.  Do not grow wrappers around one call site.
- Delete unused code.  Do not add compatibility shims before the package has a
  released compatibility contract.

## Error Handling and Testing

- Redis `-ERR` responses must signal `redis-error`; do not return error strings
  as ordinary values.
- Protocol parse failures must signal `redis-protocol-error`.
- Connection and timeout failures must surface as explicit Redis error
  conditions.
- Tests must cover the public path where practical.  Parser tests should use
  exact byte strings and assert distinguishable values.
- Do not add silent fallbacks for unsupported Redis features.

## Architecture

- Public symbols use the `redis-` prefix.
- Private symbols use the `redis--` prefix and are not caller APIs.
- Callers should use `redis-connect`, `redis-command`, `redis-disconnect`, and
  `redis-live-p`.
- Binary bulk strings are returned as unibyte strings.  Callers that want display
  text should decode them explicitly with `redis-decode-string`.
- This package does not support cluster redirection, pub/sub message loops,
  pipelining, transactions, RESP3, Sentinel, TLS, or stream consumer workflow
  yet.  Add those only with tests and a design note.

## MELPA Shape

- First line: `;;; file.el --- Short description -*- lexical-binding: t; -*-`
- Main file package metadata lives in `redis.el`.
- Split implementation files, if ever added, must not carry package metadata and
  must include SPDX license metadata.
- Keep the MELPA checklist attribution in the main package file when AI tools
  materially assist the package:
  `;; Assisted-by: OpenAI Codex:gpt-5.5`
- Last line: `;;; file.el ends here`
