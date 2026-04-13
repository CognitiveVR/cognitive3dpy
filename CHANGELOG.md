# CHANGELOG

<!-- version list -->

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
