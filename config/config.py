# config/config.py
from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field
import yaml


class PathsConfig(BaseSettings):
    input_folder: Path = Field(default=Path("data/raw_html_data"))
    manual_cleaned_html_data: Path = Field(
        default=Path("data/manual_cleaned_html_data")
    )
    consolidated_dir: Path = Field(default=Path("data/consolidated"))
    log_dir: Path = Field(default=Path("logs"))
    decomposed_output_dir: Path = Field(default=Path("data/decomposed"))
    condensed_output_dir: Path = Field(default=Path("data/condensed"))
    processing_output_dir: Path = Field(default=Path("data/processed"))
    import_files_dir: Path = Field(default=Path("data/import_files"))


class ProcessingConfig(BaseSettings):
    max_files: int = Field(default=35000)
    similarity_threshold: float = Field(default=0.99)
    max_workers: int = Field(default=16)


class FilesConfig(BaseSettings):
    data_pickle: str = Field(default="data.pickle")
    structure_pickle: str = Field(default="structure.pickle")
    filtered_data_pickle: str = Field(default="filtered_data.pickle")
    filtered_structure_pickle: str = Field(default="filtered_structure.pickle")
    data_yaml: str = Field(default="data.yaml")
    structure_yaml: str = Field(default="structure.yaml")
    filtered_data_yaml: str = Field(default="filtered_data.yaml")
    filtered_structure_yaml: str = Field(default="filtered_structure.yaml")


class DatabaseConfig(BaseSettings):
    consolidated_html_db: Path = Field(
        default=Path("data/consolidated/consolidated_html_data.db")
    )
    crawler_db: Path = Field(default=Path("data/consolidated/crawler_state.db"))


class MemgraphConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=7687)
    batch_size: int = Field(default=1000)


class LoggingConfig(BaseSettings):
    processor_log: str = Field(default="html_processor.log")
    csv_prep_log: str = Field(default="yaml_csv_prep.log")
    memgraph_importer_log: str = Field(default="memgraph_importer.log")
    unwrap_log: str = Field(default="unwrap_matching_nodes.log")
    console_level: str = Field(default="INFO")
    file_level: str = Field(default="DEBUG")


class Config(BaseSettings):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    files: FilesConfig = Field(default_factory=FilesConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    memgraph: MemgraphConfig = Field(default_factory=MemgraphConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @classmethod
    def load_from_yaml(cls, yaml_path: Path) -> "Config":
        with yaml_path.open("r") as f:
            yaml_data = yaml.safe_load(f)
        return cls(**yaml_data)


# Create a global config instance
config = Config.load_from_yaml(Path(__file__).parent / "config.yaml")
