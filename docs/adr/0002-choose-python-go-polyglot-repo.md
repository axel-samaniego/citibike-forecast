# 2. Choose Python + Go polyglot repo

Date: 2025-06-04

## Status

Accepted

## Context

We need fast dev cycles for ML (Python) **and** small static binaries for ingestion (Go).

## Decision

Use Python 3.10 for ML / APIs, Go 1.22 for network-bound micro-services.
Define data contracts in Protobuf to keep the boundary language-agnostic.

## Consequences

+ + Dev velocity in familiar ML stack.
+ + Tiny,<10 MB ingestion container.
− − Polyglot CI setup and two toolchains.