"""Pipeline-specific exceptions."""

class PipelineError(Exception):
    """Base class for expected pipeline failures."""

class SchemaValidationError(PipelineError):
    """An input CSV does not have the required columns."""

class QualityGateError(PipelineError):
    """The batch failed its row-count or reject-rate quality checks."""

class PublishError(PipelineError):
    """Publishing could not complete safely."""

class ReconciliationError(PipelineError):
    """Published data did not reconcile with the expected batch."""

class TransientError(PipelineError):
    """An error that is safe to retry, such as a temporary lock."""

class TaskTimeout(PipelineError):
    """A task exceeded its configured timeout."""
