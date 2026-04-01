"""Minimal harness smoke test for auto_dev_orchestrator py_compile + pytest step."""


def test_harness_package_importable():
    import harness

    assert hasattr(harness, "Harness")
