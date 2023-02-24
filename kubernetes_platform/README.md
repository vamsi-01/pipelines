# Kubernetes platform-specific feature

Contains protos, non-proto Python library code, and tools for generating Python <!-- and Go  --> library code from protos.

## Generate Python package, with protos
1. Update version in `python/setup.py` if applicable.
2. `make clean-python python`

If you get an error `error: invalid command 'bdist_wheel'`, run `pip install wheel` then try again.
