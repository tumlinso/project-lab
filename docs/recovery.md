# Inspect and recover sessions

Sessions, proposals, scheduler job IDs, result receipts, and captured source
identities are durable. A timeout while a scheduler operation is in flight is
an uncertain outcome: inspect the recorded scheduler job before taking another
action. Project-Lab does not blindly re-submit an effect whose outcome is
unknown.

`status` is read-only. `control` can pause, cancel, or safely resume a session
when its recorded state allows it. Importing historical session data preserves
its old identifiers and provenance; an interrupted session remains inspectable
and is never replayed automatically.

Experiment candidates and generated artifacts are separate from the canonical
working tree. Reviewing or applying a candidate is a deliberate operation
outside the experiment run.
