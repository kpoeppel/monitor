from __future__ import annotations
from compoconf import RegistrableConfigInterface, register_interface

@register_interface
class MonitorInterface(RegistrableConfigInterface):
    """Monitoring implementation responsible for observing job execution."""
