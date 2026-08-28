from __future__ import annotations

import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _local_markdown_targets(markdown: str) -> list[str]:
    targets = re.findall(r"\[[^\]]*\]\(([^)]+)\)", markdown)
    return [
        target.split("#", 1)[0]
        for target in targets
        if target
        and not target.startswith(("http://", "https://", "mailto:", "#"))
    ]


def test_readme_presents_the_current_public_boundary() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())

    for required in (
        "status-P6B.2%20verified",
        "P6C, the first authorization-gated live AWS",
        "Phase 1` through `Phase 7D",
        "P4` onward names the later operational-lifecycle program",
        "Kubernetes is not a certified deployment target",
        "is a retained historical research artifact",
        "bash deploy/aws/scripts/offline-check.sh",
        "python -m compileall agent api demo_provider runtime_service domains examples scripts",
        "ruff check agent api demo_provider runtime_service tests domains examples scripts",
        "python scripts/check_substrate_contract.py",
        "git diff --check",
        "docker compose config --quiet",
    ):
        assert required in normalized_readme

    assert "single-replica Kubernetes manifest" not in readme
    assert not (ROOT / "deploy/k8s/runtime.yaml").exists()


def test_withdrawn_kubernetes_coordinates_do_not_remain_in_public_markdown() -> None:
    forbidden = ("deploy/k8s", "travel-agent-runtime-auth", "api-keys.json")
    excluded_roots = {".git", ".mypy_cache", ".pytest_cache", ".venv"}

    for document in ROOT.rglob("*.md"):
        relative = document.relative_to(ROOT)
        if relative.parts[0] in excluded_roots:
            continue
        content = document.read_text(encoding="utf-8")
        for marker in forbidden:
            assert marker not in content, f"{relative}: {marker}"


def test_readme_local_markdown_links_resolve() -> None:
    readme_path = ROOT / "README.md"
    for target in _local_markdown_targets(readme_path.read_text(encoding="utf-8")):
        assert (readme_path.parent / target).exists(), target


def test_documentation_index_covers_every_document() -> None:
    index_path = ROOT / "docs/README.md"
    index = index_path.read_text(encoding="utf-8")

    assert "Current scope and operational contracts" in index
    assert "Executable proof records" in index
    assert "Capability and milestone records" in index
    for document in (ROOT / "docs").glob("*.md"):
        if document != index_path:
            assert f"({document.name})" in index, document.name


def test_documentation_index_local_links_resolve() -> None:
    index_path = ROOT / "docs/README.md"
    for target in _local_markdown_targets(index_path.read_text(encoding="utf-8")):
        assert (index_path.parent / target).exists(), target


def test_rl_archive_is_not_a_supported_training_claim() -> None:
    archive = (ROOT / "rl/README.md").read_text(encoding="utf-8")

    assert "historical research artifact" in archive
    assert "CI does not run the training scripts" in archive
    assert "not a supported or reproducible product path" in archive
