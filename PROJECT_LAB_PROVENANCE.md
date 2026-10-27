# Extraction provenance

Project-Lab was developed as an independent application from the Project
Control checkpoint `c14ad76cef2de02c6a70c99b072eb81d9c5304a9`. The table below
records the LAB-specific source paths reviewed as extraction references and
where their behavior was adapted or replaced.

| Original Project Control path at `c14ad76cef2de02c6a70c99b072eb81d9c5304a9` | Result here |
|---|---|
| `src/project_control/assistance/lab.py` | `src/project_lab/service.py`, `store.py`, `compat.py` |
| `src/project_control/assistance/lab_session.py` | Session state and bounded orchestration in `src/project_lab/service.py` |
| `src/project_control/assistance/lab_snapshot.py` | `src/project_lab/adapters.py` and `project-source` integration |
| `src/project_control/assistance/lab_proposals.py` | Planner contract in `src/project_lab/adapters.py`; validation in `service.py` |
| `src/project_control/assistance/lab_cli.py` | `src/project_lab/cli.py`; the same application operations are exposed through `src/project_lab/mcp.py` |
| `src/project_control/assistance/lab_gpu.py` | Host-specific GPU handling was replaced by Device Scheduler resource requests |
| `src/project_control/assistance/lab_runner.py` | Host runner and physical supervision were replaced by scheduler-supervised controller and effect jobs |
| `tests/assistance/test_lab.py`, `test_lab_session.py` | Behavior adapted to deterministic service tests in `tests/test_service.py` |
| `tests/assistance/test_lab_proposals.py`, `test_lab_snapshot.py` | Relevant validation and capture behavior adapted to `tests/test_service.py` and `tests/test_adapters.py` |
| `tests/assistance/test_lab_cli.py` | Not ported one-to-one; the extracted repository does not yet have a dedicated CLI regression test |
| `tests/assistance/test_lab_gpu.py`, `test_lab_runner.py`, `test_lab_qualification.py` | Host-runner qualification was not copied; scheduler job contracts are covered in `tests/test_scheduler_contract.py` |
| `scripts/qualify_assistance_scoped_lab.py` | Retired with the old Project Control inference/host-execution path; not copied |

The extraction preserves useful behavior such as immutable source-bound
captures, strict proposal validation, candidate isolation, bounded sessions,
durable effect intent, receipts, and conservative recovery. It does not import
Project Control, Project Control's inference runtime, or FrameShift. Device
Scheduler remains the execution authority for submitted jobs; Project-Lab
records experiment state and outcomes.

## License and notices

The referenced checkpoint contains no `LICENSE`, `NOTICE`, `COPYING`, or
`COPYRIGHT` file, and the LAB source modules listed above have no explicit
license headers. No separate source notice text was available to carry into
this extraction. This file records source provenance; it is not a license grant
or a substitute for selecting a license for the new Project-Lab repository.
Repository maintainers must make that publication decision separately. If
verbatim material is added from another source later, preserve and document its
applicable license and notices with that material.
