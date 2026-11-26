# Designer role tool manifest candidate

`flow-role-tool-manifest` is the shared, address-independent compiler for a
Claude role's tool manifest.  Its input is the tool-definition vector produced
by a trusted no-turn catalog adapter.  It verifies the complete Claude 2.1.331
catalog by definition names, canonical byte count and SHA-256 before producing
arguments or a receipt.

```sh
flow-role-tool-manifest compile Designer catalog.json receipt.json
```

The Designer manifest contains the eleven attested definitions other than
`Agent` and `Artifact`, and returns their exact `--tools` argument.  Publisher
and Other preserve all thirteen attested definitions.  The receipt is created
with `create_new`, flushed and synchronized before the result is printed.  A
catalog or schema mismatch returns `RoleToolManifestRefused`, creates no
receipt and supplies no arguments.  Existing tool-selection arguments are
refused by `integrate_manifest_arguments`, so a launcher cannot record this
manifest while running another one.

The current Flow launch schema has no model-role field and Flow has no trusted
no-turn Claude catalog producer.  Consequently this candidate does not infer a
role from an Address and is not wired into production birth or resume.  Those
paths need one role value from the shared definition and one catalog adapter,
then must call this compiler before spawning Claude.  Direct/manual/resume
adapters use the same executable and receipt rather than carrying their own
tool lists.

The catalog and filtered byte counts describe compact canonical JSON after
parsing.  They are not measurements of the provider wire, model-visible
context, tokens, or prompt cost.  A positive model-visible inventory requires
an independent no-provider oracle before deployment.
