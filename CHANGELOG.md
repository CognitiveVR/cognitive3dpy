# CHANGELOG

<!-- version list -->

## v1.2.0 (2026-04-15)

### Features

- Add data-availability flags to compact columns
  ([`f0299b3`](https://github.com/CognitiveVR/cognitive3dpy/commit/f0299b3e50361c2cb032b0c650c18906a32ad311))


## v1.1.0 (2026-04-15)

### Bug Fixes

- Added deprecation warning for unused hmd property
  ([`aa03451`](https://github.com/CognitiveVR/cognitive3dpy/commit/aa03451641ac5595988d0db1543e79daa1f861d9))

- Added function for users to clear all cache or cache for a specific project
  ([`bc3f0de`](https://github.com/CognitiveVR/cognitive3dpy/commit/bc3f0dee488a6e01bf0f2dc952482700596694d3))

- Additional guardrails for collision handling when getting events
  ([`8a2bcad`](https://github.com/CognitiveVR/cognitive3dpy/commit/8a2bcadde34e6a4fdcb51fd1ff0a427f6defd53d))

- Checkout cvr-slicer inside workspace for CI schema validation
  ([`27ac43b`](https://github.com/CognitiveVR/cognitive3dpy/commit/27ac43bed528dfcc68ae28db06c54c616a86c546))

- Ignore timestamp line in schema freshness check
  ([`de5ecd5`](https://github.com/CognitiveVR/cognitive3dpy/commit/de5ecd5bc4f3640491d00ea1847d021b161d0a15))

- Import UTC correctly in sync_schema.py
  ([`3fb5ad9`](https://github.com/CognitiveVR/cognitive3dpy/commit/3fb5ad96e67273b7af425490b94f711e3e7740d3))

- Removed dead code and updated tests
  ([`0a5c6ec`](https://github.com/CognitiveVR/cognitive3dpy/commit/0a5c6ece3dd4d5a5d0719480df305f6cdc93eab0))

- Removed unused event schema generation code
  ([`e50b3da`](https://github.com/CognitiveVR/cognitive3dpy/commit/e50b3da58c0d26295fe7a251fb754357e0bd1fa6))

- Use content hash instead of timestamp for schema freshness check
  ([`4e5689f`](https://github.com/CognitiveVR/cognitive3dpy/commit/4e5689f37f6dd0c753dc1bc39d2f545fab67fa8e))

- Use YAML mtime for schema timestamp so CI freshness check is deterministic
  ([`cc5a8d2`](https://github.com/CognitiveVR/cognitive3dpy/commit/cc5a8d2bb30bb521f0cb2713cc24e01e5e4d3d17))

### Features

- Added warning logging for deprecated fields/properties
  ([`6834ac6`](https://github.com/CognitiveVR/cognitive3dpy/commit/6834ac60c927e124cac7fbad3397e862142a1982))


## v1.0.6 (2026-04-13)


## v1.0.5 (2026-04-13)

### Bug Fixes

- Added schema for type casting known columns
  ([`1f8413a`](https://github.com/CognitiveVR/cognitive3dpy/commit/1f8413a08df252a78195a57f2591cc2fb9752fe6))

- Resolve lint errors in generated schema and transform module
  ([`d5ab0b2`](https://github.com/CognitiveVR/cognitive3dpy/commit/d5ab0b20a21bd9a51a19410020535f20168b9db4))


## v1.0.4 (2026-04-01)

### Bug Fixes

- Source hmd column from properties instead of legacy top-level field
  ([`aa58c74`](https://github.com/CognitiveVR/cognitive3dpy/commit/aa58c740b88540f3c12f1f25010f412b565a5091))


## v1.0.3 (2026-04-01)

### Bug Fixes

- Cast Null-typed columns to String to prevent downstream schema errors
  ([`a5dc021`](https://github.com/CognitiveVR/cognitive3dpy/commit/a5dc021515260ea535f421efab4176f2df62e172))


## v1.0.2 (2026-03-31)

### Bug Fixes

- Drop duplicate columns instead of suffixing with _2
  ([`cf98c32`](https://github.com/CognitiveVR/cognitive3dpy/commit/cf98c3253adf8ba2e0f3ed1007e1044621d05cc8))

- Handle duplicate column names in normalize_columns()
  ([`c4d95e9`](https://github.com/CognitiveVR/cognitive3dpy/commit/c4d95e9678886bdd22b8ff309b31331d4db00925))


## v1.0.1 (2026-03-12)

### Bug Fixes

- Type casting of expected numeric fields to Flot64 types
  ([`e556b96`](https://github.com/CognitiveVR/cognitive3dpy/commit/e556b96b00bdd557b57a5ca1fdbff55225ac0db0))

- Update version
  ([`8e4ae7c`](https://github.com/CognitiveVR/cognitive3dpy/commit/8e4ae7c0e233607286e6888081679cf2d1cf1141))

- Updated ci/cd to only publish to pypi when there's a version change
  ([`191bb6b`](https://github.com/CognitiveVR/cognitive3dpy/commit/191bb6b3b56b126cea5c0eeed1c206a73111fd73))

### Continuous Integration

- Enable automated PyPI publishing
  ([`cc6a126`](https://github.com/CognitiveVR/cognitive3dpy/commit/cc6a126eafce26fab68af74cb4a5578240a911c6))


## v1.0.0 (2026-03-11)

- Initial Release
