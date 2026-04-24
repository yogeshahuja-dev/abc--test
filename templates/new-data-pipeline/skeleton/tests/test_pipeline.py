# Tests for ${{ values.pipelineName }}

def test_pipeline_exists():
    """Basic test to verify pipeline module exists"""
    from src.pipeline import run_pipeline
    assert run_pipeline is not None

def test_pipeline_name():
    """Verify pipeline name"""
    pipeline_name = "${{ values.pipelineName }}"
    assert len(pipeline_name) > 0