#!/usr/bin/env python3

from __future__ import annotations

import json
import re
import shlex
import sys
import tomllib
from pathlib import Path
from urllib.parse import unquote


ROOT = Path(__file__).resolve().parents[1]
DOCUMENTS = [
    ROOT / "README.md",
    ROOT / "docs/architecture/arrowcol-platform/README.md",
    ROOT / "docs/architecture/arrowcol-platform/routing.md",
    ROOT / "docs/architecture/arrowcol-platform/catalog.md",
    ROOT / "docs/architecture/arrowcol-platform/delivery-guarantees.md",
    ROOT / "docs/architecture/arrowcol-platform/sdk-generation.md",
    ROOT / "docs/architecture/arrowcol-platform/storage.md",
    ROOT / "docs/architecture/arrowcol-platform/observability.md",
    ROOT / "docs/architecture/loadtest/README.md",
    ROOT / "docs/delivery-verification.md",
    ROOT / "docs/quiver/README.md",
    ROOT / "docs/containers/publishing.md",
    ROOT / "projects/arrowcol-infra/README.md",
    ROOT / "projects/arrowcol-rust/README.md",
    ROOT / "projects/arrowcol-router-rust/README.md",
    ROOT / "projects/arrowcol-dataflow-rust/README.md",
    ROOT / "projects/loadtest/README.md",
    ROOT / "projects/otelcol-rust/README.md",
    ROOT / "bin/quiver-cli/README.md",
    ROOT / "crates/arrow-deltalake/README.md",
    ROOT / "crates/arrowcol-buffer/README.md",
    ROOT / "crates/arrowcol-catalog/README.md",
    ROOT / "crates/arrowcol-dataflow-components/README.md",
    ROOT / "crates/arrowcol-flight/README.md",
    ROOT / "crates/arrowcol-protocol/README.md",
    ROOT / "crates/arrowcol-receipts/README.md",
    ROOT / "crates/arrowcol-routing/README.md",
    ROOT / "crates/arrowcol-sdk-gen/README.md",
    ROOT / "crates/otel-deltalake-exporter/README.md",
    ROOT / "tools/libs/arrowcol-sdk-gen/README.md",
]
RUNTIME_ROOTS = (
    "onelake/",
    "target/",
    ".temp/",
    "crates/arrowcol-sdk-gen/.temp/",
    "tools/libs/arrowcol-sdk-gen/.temp/",
    "projects/loadtest/.temp/",
    "projects/arrowcol-rust/.data/",
)
PATH_PREFIXES = (
    ".devcontainer/",
    ".github/",
    "bin/",
    "crates/",
    "docs/",
    "projects/",
    "submodules/",
    "tools/",
)
PROHIBITED = {
    "absolute workspace path": re.compile("/" + "workspaces" + "/"),
    "pull request URL": re.compile(r"https?://github\.com/[^)\s]+/pull/\d+", re.IGNORECASE),
    "issue URL": re.compile(r"https?://github\.com/[^)\s]+/issues/\d+", re.IGNORECASE),
    "private development branch": re.compile("dev/" + "mdrrahman" + "/", re.IGNORECASE),
}


def github_slug(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^\w\-\s]", "", value)
    return re.sub(r"\s+", "-", value)


def heading_anchors(text: str) -> set[str]:
    anchors: set[str] = set()
    counts: dict[str, int] = {}
    fenced = False
    for line in text.splitlines():
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        match = re.match(r"^#{1,6}\s+(.+?)\s*#*$", line)
        if not match:
            continue
        base = github_slug(match.group(1))
        count = counts.get(base, 0)
        counts[base] = count + 1
        anchors.add(base if count == 0 else f"{base}-{count}")
    return anchors


def load_nx_projects() -> dict[str, set[str]]:
    projects: dict[str, set[str]] = {}
    for path in ROOT.rglob("project.json"):
        if any(part in {"node_modules", "target", "submodules", "onelake", ".temp"} for part in path.parts):
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        name = data.get("name")
        if isinstance(name, str):
            projects[name] = set(data.get("targets", {}).keys())
    return projects


def load_cargo_packages() -> set[str]:
    packages: set[str] = set()
    for path in ROOT.rglob("Cargo.toml"):
        if any(part in {"target", "submodules", "onelake", ".temp"} for part in path.parts):
            continue
        with path.open("rb") as handle:
            data = tomllib.load(handle)
        package = data.get("package")
        if isinstance(package, dict) and isinstance(package.get("name"), str):
            packages.add(package["name"])
    return packages


def markdown_links(path: Path, text: str) -> list[tuple[str, str | None]]:
    links: list[tuple[str, str | None]] = []
    for raw in re.findall(r"(?<!!)\[[^\]]+\]\(([^)]+)\)", text):
        target = raw.strip().split(maxsplit=1)[0].strip("<>")
        if re.match(r"^(?:https?|mailto):", target):
            continue
        decoded = unquote(target)
        path_part, separator, anchor = decoded.partition("#")
        links.append((path_part, anchor if separator else None))
    return links


def validate_link(source: Path, path_part: str, anchor: str | None) -> None:
    target = source if not path_part else (source.parent / path_part).resolve()
    if not target.exists():
        raise ValueError(f"{source.relative_to(ROOT)}: missing link target {path_part}")
    if anchor is None or not target.is_file() or target.suffix.lower() != ".md":
        return
    anchors = heading_anchors(target.read_text(encoding="utf-8"))
    if anchor not in anchors:
        raise ValueError(
            f"{source.relative_to(ROOT)}: missing anchor #{anchor} in {target.relative_to(ROOT)}"
        )


def shell_blocks(text: str) -> list[str]:
    return re.findall(r"```(?:bash|sh)\n(.*?)```", text, flags=re.DOTALL)


def static_path_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for block in shell_blocks(value):
        flattened = re.sub(r"\\\n\s*", " ", block)
        try:
            words = shlex.split(flattened, comments=True)
        except ValueError as error:
            raise ValueError(f"invalid shell example: {error}") from error
        for word in words:
            candidate = word.rstrip(",:;")
            if not candidate.startswith(PATH_PREFIXES):
                continue
            if any(marker in candidate for marker in ("$", "<", ">", "*", "{", "}")):
                continue
            if (
                candidate.startswith(RUNTIME_ROOTS)
                or "/secrets/" in candidate
                or candidate.endswith("/secrets")
            ):
                continue
            tokens.add(candidate)
    for candidate in re.findall(r"`([^`\n]+)`", value):
        candidate = candidate.rstrip("/")
        if not candidate.startswith(PATH_PREFIXES):
            continue
        if any(marker in candidate for marker in ("$", "<", ">", "*", "{", "}", " ")):
            continue
        if (
            candidate.startswith(RUNTIME_ROOTS)
            or "/secrets/" in candidate
            or candidate.endswith("/secrets")
        ):
            continue
        tokens.add(candidate)
    return tokens


def validate_commands(
    path: Path,
    text: str,
    nx_projects: dict[str, set[str]],
    cargo_packages: set[str],
) -> tuple[int, int, int]:
    nx_count = 0
    cargo_count = 0
    path_count = 0
    for block in shell_blocks(text):
        flattened = re.sub(r"\\\n\s*", " ", block)
        for project, target in re.findall(
            r"\bnpx(?:\s+--no-install)?\s+nx\s+run\s+([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+)",
            flattened,
        ):
            if project not in nx_projects or target not in nx_projects[project]:
                raise ValueError(f"{path.relative_to(ROOT)}: unknown Nx target {project}:{target}")
            nx_count += 1
        for command in re.findall(r"\bnpx(?:\s+--no-install)?\s+nx\s+run-many\b[^;\n]*", flattened):
            projects_match = re.search(r"--projects(?:=|\s+)([A-Za-z0-9_.,-]+)", command)
            target_match = re.search(r"(?:^|\s)(?:-t|--target)(?:=|\s+)([A-Za-z0-9_.-]+)", command)
            if not projects_match or not target_match:
                continue
            target = target_match.group(1)
            for project in projects_match.group(1).split(","):
                if project not in nx_projects or target not in nx_projects[project]:
                    raise ValueError(f"{path.relative_to(ROOT)}: unknown Nx target {project}:{target}")
                nx_count += 1
        for package in re.findall(
            r"\bcargo\b[^\n;]*(?:--package|-p)\s+([A-Za-z0-9_.-]+)",
            flattened,
        ):
            if package not in cargo_packages:
                raise ValueError(f"{path.relative_to(ROOT)}: unknown Cargo package {package}")
            cargo_count += 1
    for candidate in static_path_tokens(text):
        target = ROOT / candidate
        if not target.exists():
            raise ValueError(f"{path.relative_to(ROOT)}: missing documented path {candidate}")
        path_count += 1
    return nx_count, cargo_count, path_count


def validate_markdown(path: Path, text: str) -> None:
    if not text.endswith("\n"):
        raise ValueError(f"{path.relative_to(ROOT)}: missing final newline")
    fenced = False
    previous_heading = 0
    for number, line in enumerate(text.splitlines(), start=1):
        if line != line.rstrip():
            raise ValueError(f"{path.relative_to(ROOT)}:{number}: trailing whitespace")
        if "\t" in line:
            raise ValueError(f"{path.relative_to(ROOT)}:{number}: tab character")
        if line.lstrip().startswith("```"):
            fenced = not fenced
            continue
        if fenced:
            continue
        heading = re.match(r"^(#{1,6})\s+", line)
        if heading:
            level = len(heading.group(1))
            if previous_heading and level > previous_heading + 1:
                raise ValueError(f"{path.relative_to(ROOT)}:{number}: skipped heading level")
            previous_heading = level
    if fenced:
        raise ValueError(f"{path.relative_to(ROOT)}: unclosed fenced code block")
    for name, pattern in PROHIBITED.items():
        if pattern.search(text):
            raise ValueError(f"{path.relative_to(ROOT)}: prohibited {name}")


def main() -> int:
    nx_projects = load_nx_projects()
    cargo_packages = load_cargo_packages()
    link_count = 0
    nx_count = 0
    cargo_count = 0
    path_count = 0
    try:
        for path in DOCUMENTS:
            text = path.read_text(encoding="utf-8")
            validate_markdown(path, text)
            for path_part, anchor in markdown_links(path, text):
                validate_link(path, path_part, anchor)
                link_count += 1
            nx, cargo, paths = validate_commands(path, text, nx_projects, cargo_packages)
            nx_count += nx
            cargo_count += cargo
            path_count += paths
    except (OSError, ValueError, json.JSONDecodeError, tomllib.TOMLDecodeError) as error:
        print(f"documentation validation failed: {error}", file=sys.stderr)
        return 1
    print(
        "documentation validation passed: "
        f"{len(DOCUMENTS)} files, {link_count} links, {nx_count} Nx targets, "
        f"{cargo_count} Cargo package references, {path_count} paths"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
