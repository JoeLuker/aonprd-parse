from pathlib import Path
from pydantic_settings import BaseSettings
from pydantic import Field, ConfigDict
import yaml

class PathsConfig(BaseSettings):
    input_folder: Path = Field(default=Path("input_data/raw_html_data"))
    cleaned_html_data: Path = Field(default=Path("output_data/consolidated/unique_raw_html_data"))
    manual_cleaned_html_data: Path = Field(default=Path("output_data/manual_cleaned_html_data"))
    consolidated_dir: Path = Field(default=Path("output_data/consolidated"))
    log_dir: Path = Field(default=Path("logs"))
    decomposed_output_dir: Path = Field(default=Path("output_data/decomposed"))
    condensed_output_dir: Path = Field(default=Path("output_data/condensed"))
    processing_output_dir: Path = Field(default=Path("output_data/processed"))
    processed_output_dir: Path = Field(default=Path("output_data/processed")) 
    import_files_dir: Path = Field(default=Path("output_data/import_files"))

    model_config = ConfigDict(arbitrary_types_allowed=True)

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
    consolidated_html_db: Path = Field(default=Path("output_data/consolidated/consolidated_html_data.db"))
    crawler_db: Path = Field(default=Path("input_data/crawler_state.db"))
    model_config = ConfigDict(arbitrary_types_allowed=True)

class MemgraphConfig(BaseSettings):
    host: str = Field(default="localhost")
    port: int = Field(default=7687)
    batch_size: int = Field(default=1000)

class Config(BaseSettings):
    paths: PathsConfig = Field(default_factory=PathsConfig)
    processing: ProcessingConfig = Field(default_factory=ProcessingConfig)
    files: FilesConfig = Field(default_factory=FilesConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    memgraph: MemgraphConfig = Field(default_factory=MemgraphConfig)

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        arbitrary_types_allowed=True
    )

    @classmethod
    def load_from_yaml(cls, yaml_path: Path) -> "Config":
        with yaml_path.open("r") as f:
            yaml_data = yaml.safe_load(f)
        return cls(**yaml_data)

# Create a global config instance
config = Config.load_from_yaml(Path(__file__).parent / "config.yml")