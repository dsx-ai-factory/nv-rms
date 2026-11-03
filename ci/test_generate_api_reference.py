# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
"""Tests for the RMS gRPC API reference generator."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path
from unittest import mock

import generate_api_reference as gen


def _image() -> dict:
    """A small buf-style image with one sync and one async RPC."""

    def loc(path: list[int], leading: str = "", trailing: str = "") -> dict:
        entry: dict = {"path": path}
        if leading:
            entry["leadingComments"] = leading
        if trailing:
            entry["trailingComments"] = trailing
        return entry

    string_field = lambda name, number: {  # noqa: E731
        "name": name,
        "number": number,
        "label": "LABEL_OPTIONAL",
        "type": "TYPE_STRING",
    }
    return {
        "file": [
            {
                "name": "demo.proto",
                "package": "demo",
                "messageType": [
                    {"name": "PingRequest", "field": [string_field("node_id", 1)]},
                    {
                        "name": "PingResponse",
                        "field": [
                            {
                                "name": "state",
                                "number": 1,
                                "label": "LABEL_OPTIONAL",
                                "type": "TYPE_ENUM",
                                "typeName": ".demo.State",
                            }
                        ],
                    },
                    {"name": "StartRequest", "field": [string_field("node_id", 1)]},
                    {
                        "name": "StartResponse",
                        "field": [string_field("job_id", 1)],
                    },
                    {
                        "name": "Holder",
                        "field": [
                            {
                                "name": "labels",
                                "number": 1,
                                "label": "LABEL_REPEATED",
                                "type": "TYPE_MESSAGE",
                                "typeName": ".demo.Holder.LabelsEntry",
                            },
                            {
                                "name": "name",
                                "number": 2,
                                "label": "LABEL_OPTIONAL",
                                "type": "TYPE_STRING",
                                "oneofIndex": 0,
                            },
                            {
                                "name": "id",
                                "number": 3,
                                "label": "LABEL_OPTIONAL",
                                "type": "TYPE_STRING",
                                "oneofIndex": 0,
                            },
                            {
                                "name": "note",
                                "number": 4,
                                "label": "LABEL_OPTIONAL",
                                "type": "TYPE_STRING",
                                "oneofIndex": 1,
                                "proto3Optional": True,
                            },
                        ],
                        "oneofDecl": [{"name": "who"}, {"name": "_note"}],
                        "nestedType": [
                            {
                                "name": "LabelsEntry",
                                "field": [
                                    string_field("key", 1),
                                    string_field("value", 2),
                                ],
                                "options": {"mapEntry": True},
                            }
                        ],
                    },
                ],
                "enumType": [
                    {
                        "name": "State",
                        "value": [
                            {"name": "STATE_UNSPECIFIED", "number": 0},
                            {"name": "STATE_ON", "number": 1},
                        ],
                    }
                ],
                "service": [
                    {
                        "name": "Demo",
                        "method": [
                            {
                                "name": "Ping",
                                "inputType": ".demo.PingRequest",
                                "outputType": ".demo.PingResponse",
                            },
                            {
                                "name": "Start",
                                "inputType": ".demo.StartRequest",
                                "outputType": ".demo.StartResponse",
                            },
                        ],
                    }
                ],
                "sourceCodeInfo": {
                    "location": [
                        loc([6, 0, 2, 0], "\n Ping reads state. It is fast.\n"),
                        loc(
                            [6, 0, 2, 1],
                            "\n Start asynchronously starts a job.\n Poll Ping.\n",
                        ),
                        loc([4, 0, 2, 0], trailing=" Node to ping"),
                        loc([5, 0, 2, 1], trailing=" Power is on"),
                    ]
                },
            }
        ]
    }


def _metadata() -> dict:
    return {
        "categories": {"demo": {"title": "Demo", "summary": "Demo RPCs."}},
        "rpcs": {
            "Demo/Ping": {"category": "demo", "execution": "sync", "target": "none"},
            "Demo/Start": {
                "category": "demo",
                "execution": "async-job",
                "target": "registered",
                "poll_with": "Ping",
            },
        },
    }


class TextHelperTests(unittest.TestCase):
    def test_comment_blocks_join_wrapped_lines_and_keep_list_items(self) -> None:
        raw = "\n First line\n  wraps here.\n\n - one\n - two\n"
        self.assertEqual(
            gen.comment_blocks(raw),
            [
                ("para", "First line wraps here."),
                ("item", "- one"),
                ("item", "- two"),
            ],
        )

    def test_comment_blocks_keep_hyphenated_word_across_wrap(self) -> None:
        raw = " retrieves data for a caller-\n supplied list of nodes."
        self.assertEqual(
            gen.comment_blocks(raw),
            [("para", "retrieves data for a caller-supplied list of nodes.")],
        )

    def test_description_markdown_separates_paragraphs_and_lists(self) -> None:
        raw = "\n Intro text.\n\n - a\n - b\n"
        self.assertEqual(gen.description_markdown(raw), "Intro text.\n\n- a\n- b")

    def test_mdx_escape_protects_braces_and_angles_outside_code(self) -> None:
        self.assertEqual(
            gen.mdx_escape("use {x} and <y> but keep `{z}` and `<w>`"),
            "use \\{x\\} and \\<y\\> but keep `{z}` and `<w>`",
        )

    def test_cell_escapes_pipes_and_collapses_whitespace(self) -> None:
        self.assertEqual(gen.cell("a | b\n  c"), "a \\| b c")
        self.assertEqual(gen.cell(""), "-")

    def test_first_sentence_stops_at_sentence_boundary(self) -> None:
        self.assertEqual(
            gen.first_sentence("Reads state. It is fast."), "Reads state."
        )
        self.assertEqual(gen.first_sentence("No period"), "No period")

    def test_kebab_matches_fern_slugs(self) -> None:
        self.assertEqual(gen.kebab("SetPowerState"), "set-power-state")
        self.assertEqual(gen.kebab("RackManagerV2"), "rack-manager-v-2")

    def test_github_slug_handles_parentheses(self) -> None:
        self.assertEqual(
            gen.github_slug("ConfigureScaleUpFabricManager (V2)"),
            "configurescaleupfabricmanager-v2",
        )


class ModelTests(unittest.TestCase):
    def setUp(self) -> None:
        self.model = gen.build_model(_image())

    def test_rpcs_carry_comments_and_paths(self) -> None:
        ping = self.model.rpcs[0]
        self.assertEqual(ping.grpc_path, "demo.Demo/Ping")
        self.assertEqual(
            gen.comment_text(ping.description), "Ping reads state. It is fast."
        )

    def test_trailing_field_comments_become_descriptions(self) -> None:
        field = self.model.messages[".demo.PingRequest"].fields[0]
        self.assertEqual(field.description, "Node to ping")

    def test_map_fields_resolve_key_and_value(self) -> None:
        labels = self.model.messages[".demo.Holder"].fields[0]
        self.assertEqual(labels.kind, "map")
        self.assertEqual(labels.map_key, "string")
        self.assertEqual(labels.map_value, ("scalar", "string"))

    def test_oneofs_skip_synthetic_proto3_optional_groups(self) -> None:
        holder = self.model.messages[".demo.Holder"]
        self.assertEqual(holder.oneofs, {"who": ["name", "id"]})
        note = next(f for f in holder.fields if f.name == "note")
        self.assertEqual(note.label, "optional")
        self.assertIsNone(note.oneof)

    def test_enum_values_keep_numbers_and_comments(self) -> None:
        state = self.model.enums[".demo.State"]
        self.assertEqual(
            [(v.name, v.number, v.description) for v in state.values],
            [("STATE_UNSPECIFIED", 0, ""), ("STATE_ON", 1, "Power is on")],
        )

    def test_example_message_picks_one_oneof_member_and_non_zero_enum(self) -> None:
        self.assertEqual(
            gen.example_message(self.model, ".demo.Holder"),
            {"labels": {"<key>": "<labels>"}, "name": "<name>", "note": "<note>"},
        )
        self.assertEqual(
            gen.example_message(self.model, ".demo.PingResponse"),
            {"state": "STATE_ON"},
        )


class MetadataCheckTests(unittest.TestCase):
    def setUp(self) -> None:
        self.model = gen.build_model(_image())

    def test_consistent_metadata_has_no_problems(self) -> None:
        self.assertEqual(gen.check_metadata(self.model, _metadata()), [])

    def test_missing_entry_and_unknown_entry_are_reported(self) -> None:
        metadata = _metadata()
        del metadata["rpcs"]["Demo/Ping"]
        metadata["rpcs"]["Demo/Gone"] = {
            "category": "demo",
            "execution": "sync",
            "target": "none",
        }
        problems = "\n".join(gen.check_metadata(self.model, metadata))
        self.assertIn("Demo/Ping: no entry", problems)
        self.assertIn("Demo/Gone: in rpc-metadata.toml but not in the protos", problems)

    def test_sync_rpc_with_asynchronous_comment_is_flagged(self) -> None:
        metadata = _metadata()
        metadata["rpcs"]["Demo/Start"].update({"execution": "sync"})
        metadata["rpcs"]["Demo/Start"].pop("poll_with")
        problems = gen.check_metadata(self.model, metadata)
        self.assertTrue(any("says it is asynchronous" in p for p in problems))

        metadata["rpcs"]["Demo/Start"]["override_async_check"] = True
        self.assertEqual(gen.check_metadata(self.model, metadata), [])

    def test_async_rpc_needs_existing_poller_and_job_field(self) -> None:
        metadata = _metadata()
        metadata["rpcs"]["Demo/Start"]["poll_with"] = "Missing"
        metadata["rpcs"]["Demo/Ping"].update(
            {"execution": "async-job", "poll_with": "Ping"}
        )
        problems = "\n".join(gen.check_metadata(self.model, metadata))
        self.assertIn("names unknown RPC `Missing`", problems)
        self.assertIn("Demo/Ping: marked async-job, but the response has no", problems)

    def test_poll_with_on_sync_rpc_is_flagged(self) -> None:
        metadata = _metadata()
        metadata["rpcs"]["Demo/Ping"]["poll_with"] = "Ping"
        problems = gen.check_metadata(self.model, metadata)
        self.assertTrue(any("only valid for asynchronous" in p for p in problems))

    def test_invalid_values_are_reported(self) -> None:
        metadata = deepcopy(_metadata())
        metadata["rpcs"]["Demo/Ping"].update(
            {"execution": "fast", "target": "elsewhere", "category": "nope"}
        )
        problems = "\n".join(gen.check_metadata(self.model, metadata))
        self.assertIn("invalid execution `fast`", problems)
        self.assertIn("invalid target `elsewhere`", problems)
        self.assertIn("unknown category `nope`", problems)

    def test_long_first_sentence_requires_summary(self) -> None:
        image = _image()
        image["file"][0]["sourceCodeInfo"]["location"][0]["leadingComments"] = (
            " " + "word " * 60 + "end.\n"
        )
        model = gen.build_model(image)
        problems = gen.check_metadata(model, _metadata())
        self.assertTrue(any("Add a `summary`" in p for p in problems))

        metadata = _metadata()
        metadata["rpcs"]["Demo/Ping"]["summary"] = "Short."
        self.assertEqual(gen.check_metadata(model, metadata), [])


class RenderTests(unittest.TestCase):
    def setUp(self) -> None:
        self.model = gen.build_model(_image())
        self.metadata = _metadata()
        self.pages = gen.generate_pages(self.model, self.metadata)

    def test_pages_cover_catalog_categories_and_types(self) -> None:
        self.assertEqual(
            sorted(self.pages),
            ["demo.md", "rpc-catalog.md", "types-demo.md"],
        )

    def test_pages_use_lf_endings_and_notice_after_heading(self) -> None:
        for name, content in self.pages.items():
            self.assertNotIn("\r", content, name)
            self.assertTrue(content.startswith("# "), name)
            self.assertIn(gen.GENERATED_NOTICE, content, name)
            self.assertTrue(content.endswith("\n"), name)

    def test_catalog_has_service_column_and_no_summary(self) -> None:
        catalog = self.pages["rpc-catalog.md"]
        self.assertIn("| RPC Method | Service | Execution | Poll with | Target |", catalog)
        self.assertNotIn("Summary", catalog)

    def test_async_section_names_poller_and_job_field(self) -> None:
        section = self.pages["demo.md"].split("## Start")[1]
        self.assertIn("**Asynchronous, one job.**", section)
        self.assertIn("Poll [`Ping`](demo.md#ping) with the job ID in `job_id`.", section)

    def test_sync_section_has_no_job_tracking_row(self) -> None:
        section = self.pages["demo.md"].split("## Ping")[1].split("## Start")[0]
        self.assertIn("**Synchronous.**", section)
        self.assertNotIn("Job tracking", section)

    def test_catalog_lists_every_rpc_with_execution_and_poller(self) -> None:
        catalog = self.pages["rpc-catalog.md"]
        self.assertIn("| [`Ping`](demo.md#ping) | `Demo` | Synchronous | - |", catalog)
        self.assertIn(
            "| [`Start`](demo.md#start) | `Demo` | Asynchronous, one job "
            "| [`Ping`](demo.md#ping) |",
            catalog,
        )

    def test_types_page_documents_oneofs_maps_and_enums(self) -> None:
        types = self.pages["types-demo.md"]
        self.assertIn("map of `string` to `string`", types)
        self.assertIn("Set at most one of `name`, `id` (oneof `who`).", types)
        self.assertIn("| `STATE_ON` | 1 | Power is on |", types)

    def test_generation_is_deterministic(self) -> None:
        again = gen.generate_pages(self.model, self.metadata)
        self.assertEqual(self.pages, again)


class SyncTests(unittest.TestCase):
    REV = "a" * 40

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.cargo = root / "Cargo.toml"
        self.cargo.write_text(
            "[workspace.dependencies]\n"
            f'librms = {{ git = "https://example.invalid/librms", rev = "{self.REV}" }}\n'
        )
        self.vendored = root / "fern" / "proto"
        self.vendored.mkdir(parents=True)
        self.rev_file = self.vendored / "LIBRMS_REV"
        self.source = root / "librms" / "proto"
        self.source.mkdir(parents=True)
        (self.source / "a.proto").write_text("syntax = \"proto3\";\n")
        for name, value in (
            ("CARGO_TOML", self.cargo),
            ("PROTO_DIR", self.vendored),
            ("REV_FILE", self.rev_file),
        ):
            patcher = mock.patch.object(gen, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def metadata_result(self, rev: str) -> mock.Mock:
        package = {
            "name": "librms",
            "source": f"git+https://example.invalid/librms?rev={rev}#{rev}",
            "manifest_path": str(self.source.parent / "Cargo.toml"),
        }
        return mock.Mock(returncode=0, stdout=json.dumps({"packages": [package]}))

    def test_pinned_revision_is_read_from_cargo_toml(self) -> None:
        self.assertEqual(gen.pinned_librms_rev(), self.REV)

    def test_missing_pin_is_an_error(self) -> None:
        self.cargo.write_text('[workspace.dependencies]\nlibrms = "1"\n')
        with self.assertRaises(gen.ApiReferenceError):
            gen.pinned_librms_rev()

    def test_revision_check_reports_missing_and_stale_files(self) -> None:
        self.assertIn("missing", gen.check_vendored_revision()[0])
        self.rev_file.write_text("b" * 40 + "\n")
        self.assertIn("pins", gen.check_vendored_revision()[0])
        self.rev_file.write_text(gen.render_rev_file(self.REV))
        self.assertEqual(gen.check_vendored_revision(), [])

    def test_revision_check_detects_hand_edited_and_unrecorded_protos(self) -> None:
        (self.vendored / "a.proto").write_text("syntax = \"proto3\";\n")
        self.rev_file.write_text(gen.render_rev_file(self.REV))
        (self.vendored / "a.proto").write_text("edited\n")
        problems = gen.check_vendored_revision()
        self.assertEqual(len(problems), 1)
        self.assertIn("a.proto differs", problems[0])
        (self.vendored / "a.proto").write_text("syntax = \"proto3\";\n")
        (self.vendored / "extra.proto").write_text("added\n")
        self.assertIn("extra.proto differs", gen.check_vendored_revision()[0])
        (self.vendored / "extra.proto").unlink()
        (self.vendored / "a.proto").unlink()
        self.assertIn("a.proto differs", gen.check_vendored_revision()[0])

    def test_revision_check_rejects_legacy_file_without_digests(self) -> None:
        (self.vendored / "a.proto").write_text("syntax = \"proto3\";\n")
        self.rev_file.write_text(self.REV + "\n")
        self.assertIn("a.proto differs", gen.check_vendored_revision()[0])

    def test_sync_copies_protos_and_records_revision(self) -> None:
        (self.vendored / "stale.proto").write_text("old")
        with mock.patch.object(
            gen.subprocess, "run", return_value=self.metadata_result(self.REV)
        ):
            self.assertEqual(gen.cmd_sync(argparse.Namespace(verify=False)), 0)
        self.assertEqual(
            sorted(p.name for p in self.vendored.glob("*.proto")), ["a.proto"]
        )
        self.assertEqual(self.rev_file.read_text(), gen.render_rev_file(self.REV))
        self.assertEqual(gen.check_vendored_revision(), [])
        self.assertTrue(self.rev_file.read_text().startswith(self.REV + "\na.proto:"))

    def test_sync_verify_detects_drift(self) -> None:
        (self.vendored / "a.proto").write_text("changed")
        with mock.patch.object(
            gen.subprocess, "run", return_value=self.metadata_result(self.REV)
        ):
            with self.assertRaises(gen.ApiReferenceError) as raised:
                gen.cmd_sync(argparse.Namespace(verify=True))
        self.assertIn("a.proto differs", str(raised.exception))

    def test_sync_rejects_cargo_resolution_that_differs_from_pin(self) -> None:
        with mock.patch.object(
            gen.subprocess, "run", return_value=self.metadata_result("c" * 40)
        ):
            with self.assertRaises(gen.ApiReferenceError) as raised:
                gen.cmd_sync(argparse.Namespace(verify=False))
        self.assertIn("cargo update -p librms", str(raised.exception))


@unittest.skipUnless(
    shutil.which("buf") or shutil.which("npx"), "buf or npx is required"
)
class RepositoryTests(unittest.TestCase):
    """Run the checks against the real vendored protos and metadata."""

    def test_committed_reference_is_current(self) -> None:
        model = gen.build_model(gen.build_image())
        metadata = gen.load_metadata()
        self.assertEqual(gen.check_metadata(model, metadata), [])
        for name, content in gen.generate_pages(model, metadata).items():
            path = gen.OUTPUT_DIR / name
            self.assertTrue(path.exists(), f"{name} is missing")
            self.assertEqual(path.read_text(), content, f"{name} is stale")

    def test_vendored_revision_matches_cargo_pin(self) -> None:
        self.assertEqual(gen.check_vendored_revision(), [])


if __name__ == "__main__":
    unittest.main()
