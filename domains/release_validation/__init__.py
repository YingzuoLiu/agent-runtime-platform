"""Synthetic, offline release-validation workflow.

A deterministic DAG-based registered-tool workflow used to demonstrate
that `runtime_service`'s durable workflow persistence
(`SQLiteWorkflowStore`) and registered-tool sandbox (`ToolSandbox`) are
domain-agnostic: this domain has no relationship to Travel and does not
import `AgentState`. Its managed adapter satisfies Core contracts and is
registered by the service composition root. Selective replay creates a new
run and reuses only compatible completed evidence from a terminal source.

All manifests, artifacts, test results, and compatibility data here are
synthetic fixtures for this repository; nothing here reads or represents
any real organization's release process.
"""
