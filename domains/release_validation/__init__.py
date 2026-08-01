"""Synthetic, offline release-validation workflow.

A fixed-order, multi-step registered-tool workflow used to demonstrate
that `runtime_service`'s durable workflow persistence
(`SQLiteWorkflowStore`) and registered-tool sandbox (`ToolSandbox`) are
domain-agnostic: this domain has no relationship to Travel and does not
import `AgentState`, `RuntimeManager`, or `AgentRegistry`.

All manifests, artifacts, test results, and compatibility data here are
synthetic fixtures for this repository; nothing here reads or represents
any real organization's release process.
"""
