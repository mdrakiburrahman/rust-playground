from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import validate


class WorkspaceDiscoveryTests(unittest.TestCase):
    @staticmethod
    def checkout(parent: Path) -> Path:
        root = parent / "checkout"
        root.mkdir(parents=True)

        def project(directory: Path, name: str, target: str) -> None:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "project.json").write_text(
                json.dumps({"name": name, "targets": {target: {}}}), encoding="utf-8"
            )

        def package(directory: Path, name: str) -> None:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "Cargo.toml").write_text(
                f'[package]\nname = "{name}"\nversion = "0.1.0"\n', encoding="utf-8"
            )

        project(root, "fixture", "verify")
        project(root / ".devcontainer", "devcontainer", "test-source")
        package(root, "fixture-package")
        package(root / "crates/child", "fixture-child")
        for excluded in ("node_modules", "target", "submodules", "onelake", ".temp"):
            directory = root / excluded / "ignored"
            project(directory, f"ignored-{excluded}", "verify")
            if excluded != "node_modules":
                package(directory, f"ignored-{excluded}")
        return root

    @staticmethod
    def inventories(root: Path) -> tuple[dict[str, set[str]], set[str]]:
        with patch.object(validate, "ROOT", root):
            return validate.load_nx_projects(), validate.load_cargo_packages()

    def test_ancestor_temp_preserves_inventory_and_nested_runtime_exclusions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            normal = self.checkout(Path(directory) / "plain")
            nested = self.checkout(Path(directory) / ".temp")
            expected = (
                {"fixture": {"verify"}, "devcontainer": {"test-source"}},
                {"fixture-package", "fixture-child"},
            )
            self.assertEqual(self.inventories(normal), expected)
            self.assertEqual(self.inventories(nested), expected)

    def test_known_targets_and_packages_resolve_under_both_checkout_paths(self) -> None:
        text = (
            "```bash\n"
            "npx nx run devcontainer:test-source\n"
            "npx nx run fixture:verify\n"
            "cargo test --package fixture-package\n"
            "```\n"
        )
        with tempfile.TemporaryDirectory() as directory:
            for ancestor in ("plain", ".temp"):
                with self.subTest(ancestor=ancestor):
                    root = self.checkout(Path(directory) / ancestor)
                    projects, packages = self.inventories(root)
                    with patch.object(validate, "ROOT", root):
                        self.assertEqual(
                            validate.validate_commands(root / "README.md", text, projects, packages),
                            (2, 1, 0),
                        )

    def test_unknown_target_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.checkout(Path(directory) / ".temp")
            projects, packages = self.inventories(root)
            with patch.object(validate, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "unknown Nx target devcontainer:missing"):
                    validate.validate_commands(
                        root / "README.md", "```bash\nnpx nx run devcontainer:missing\n```\n",
                        projects, packages,
                    )

    def test_unknown_package_still_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.checkout(Path(directory) / ".temp")
            projects, packages = self.inventories(root)
            with patch.object(validate, "ROOT", root):
                with self.assertRaisesRegex(ValueError, "unknown Cargo package missing"):
                    validate.validate_commands(
                        root / "README.md", "```bash\ncargo test --package missing\n```\n",
                        projects, packages,
                    )

    def test_private_env_notes_do_not_require_secret_or_hide_missing_source(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.checkout(Path(directory) / ".temp")
            projects, packages = self.inventories(root)
            with patch.object(validate, "ROOT", root):
                for text in (
                    "Never commit `.devcontainer/.env`.\n",
                    "```bash\ncat .devcontainer/.env\n```\n",
                ):
                    with self.subTest(text=text):
                        self.assertEqual(
                            validate.validate_commands(root / "README.md", text, projects, packages),
                            (0, 0, 0),
                        )
                for source in (".devcontainer/.env.example", "projects/missing.yaml"):
                    with self.subTest(source=source):
                        self.assertEqual(validate.static_path_tokens(f"`{source}`"), {source})
                        with self.assertRaisesRegex(ValueError, "missing documented path"):
                            validate.validate_commands(
                                root / "README.md", f"`{source}`", projects, packages
                            )
                self.assertFalse((root / ".devcontainer/.env").exists())


if __name__ == "__main__":
    unittest.main()
