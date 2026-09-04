from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = ""
    schema: str = "retail_lakehouse"
    base_path: str = ""
    kafka_bootstrap_servers: str = ""
    kafka_topic: str = "retail-clickstream"

    def table(self, name: str) -> str:
        if self.catalog:
            return f"`{self.catalog}`.`{self.schema}`.`{name}`"
        return f"`{self.schema}`.`{name}`"

    def checkpoint(self, name: str) -> str:
        return f"{self.base_path.rstrip('/')}/checkpoints/{name}"

    def path(self, *parts: str) -> str:
        clean = "/".join(p.strip("/") for p in parts)
        return f"{self.base_path.rstrip('/')}/{clean}"
