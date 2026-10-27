# Recording experiment measurements

Project-Lab compares measurements recorded in scheduler receipts. A candidate
can report them in either of these ways:

1. Prefer an explicit `measurements` mapping in the scheduler execution result.
2. Otherwise, write one complete JSON object to standard output with a
   top-level `measurements` mapping, for example:

   ```json
   {"measurements":{"elapsed_ms":12.5,"passed":8}}
   ```

Measurement names must be non-empty strings and values must be finite JSON
numbers. Keep this object by itself on standard output; send progress text to
standard error. The proposal's `measurements` list declares which names the
candidate expects to report.

The Device Scheduler bounds captured output. Project-Lab parses standard
output only when the scheduler envelope says the output is complete, its
returned and original byte counts match, and its SHA-256 digest matches the
returned text. Truncated or unverifiable output remains available in the
receipt but does not become measurement data. Explicit result measurements
are retained with their scheduler-result provenance.

`results` includes measurements and their `measurement_provenance` beside the
job, attempt, and bounded logs. `compare` uses only those recorded measurements;
the next planner turn receives them as part of the prior effect receipt.

