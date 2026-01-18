"""slurm_gen - SLURM script generation utilities.

This library provides:
- Template rendering for SBATCH scripts
- Script validation utilities
- Configuration schema for SLURM settings

Example usage:
    from slurm_gen import SlurmConfig

    # Create a fake client for testing
    slurm_config = SlurmConfig(
        template_path="templates/job.sbatch",
        script_dir="/tmp/scripts",
        log_dir="/tmp/logs",
    )
"""

from slurm_gen.schema import (
    SlurmConfig,
    SrunConfig,
    SbatchConfig,
)
from slurm_gen.template_renderer import (
    render_template,
    render_template_file,
    SbatchTemplateError,
)
from slurm_gen.generator import (
    build_sbatch_directives,
    build_replacements,
    generate_script,
    merge_slurm_config,
)
from slurm_gen.validator import (
    validate_job_script,
    SlurmValidationError,
)

__version__ = "0.1.0"

__all__ = [
    # Schema
    "SlurmConfig",
    "SrunConfig",
    "SbatchConfig",
    # Template rendering
    "render_template",
    "render_template_file",
    "SbatchTemplateError",
    # Validation
    "validate_job_script",
    "SlurmValidationError",
    # Generation
    "build_sbatch_directives",
    "build_replacements",
    "generate_script",
    "merge_slurm_config",
]
