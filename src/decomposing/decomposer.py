import asyncio
from bs4 import BeautifulSoup, Tag, NavigableString, Comment, Doctype
from tqdm.asyncio import tqdm
from pathlib import Path
import xxhash
from threading import RLock
from typing import List, Tuple, Dict, Any
import multiprocessing
from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations


logger = Logger.get_logger("DecomposerLogger", config.paths.log_dir / "decomposer.log")

def make_hashable(obj):
    if isinstance(obj, list):
        return tuple(make_hashable(e) for e in obj)
    elif isinstance(obj, dict):
        return frozenset((k, make_hashable(v)) for k, v in obj.items())
    else:
        return obj

class Decomposer:
    def __init__(self):
        self.data = {
            'texts': {},
            'doctypes': {},
            'comments': {},
            'attributes': {}
        }
        self.structure = {
            'nodes': [],
            'edges': []
        }
        self.text_lookup = {}
        self.doctype_lookup = {}
        self.comment_lookup = {}
        self.attribute_lookup = {}
        self.node_id_counter = 1
        self.attribute_id_counter = 1
        self.subtree_hash_cache = {}
        self.subtree_node_cache = {}
        self.edge_set = set()
        self.cache_lock = RLock()
        self.input_directory = config.paths.manual_cleaned_html_data
        self.max_cores = min(20, multiprocessing.cpu_count())

    async def process(self):
        await FileOperations.ensure_directory(config.paths.decomposed_output_dir)

        files_to_process = self.get_files_to_process()
        if not files_to_process:
            logger.warning("No HTML files found to process.")
            return

        semaphore = asyncio.Semaphore(self.max_cores * 10)

        async def sem_task(file):
            async with semaphore:
                await self.process_file(file)

        tasks = [sem_task(file) for file in files_to_process]

        for task in tqdm.as_completed(tasks, total=len(tasks), desc="Processing HTML files", unit="file"):
            await task

        logger.info("Finished processing all HTML files.")

    def get_files_to_process(self) -> List[Path]:
        files = list(self.input_directory.glob('*.html'))
        logger.info(f"Found {len(files)} HTML files to process.")
        return files

    async def process_file(self, file_path: Path):
        logger.debug(f"Processing file: {file_path}")
        try:
            content = await FileOperations.read_file_async(file_path)
            soup = BeautifulSoup(content, 'html.parser')
            self._process_document(soup, file_path.name)
        except Exception as e:
            logger.error(f"Error processing file {file_path.name}: {e}", exc_info=True)

    def _process_document(self, soup: BeautifulSoup, filename: str):
        root_node_id, _ = self._create_node('document', filename=filename)
        for order, element in enumerate(soup.contents, start=1):
            child_node_id, _ = self._process_node(element, parent_id=root_node_id, order=order)
            if isinstance(element, Tag) and child_node_id:
                self._create_edge(source_id=root_node_id, target_id=child_node_id, relationship='HAS_ROOT', order=order)

    def _process_node(self, element: Any, parent_id: str, order: int) -> Tuple[str, str]:
        if isinstance(element, Doctype):
            node_id, _ = self._create_node('doctype', content=str(element))
            self._create_edge(source_id=parent_id, target_id=node_id, relationship='HAS_DOCTYPE', order=order)
        elif isinstance(element, Comment):
            node_id, _ = self._create_node('comment', content=str(element))
            self._create_edge(source_id=parent_id, target_id=node_id, relationship='HAS_COMMENT', order=order)
        elif isinstance(element, NavigableString):
            if element.strip():
                node_id, _ = self._create_node('textnode', content=element.strip())
                self._create_edge(source_id=parent_id, target_id=node_id, relationship='CONTAINS_TEXT', order=order)
            else:
                return '', ''
        elif isinstance(element, Tag):
            children_hashes = []
            for child in element.children:
                _, child_hash = self._compute_subtree_hash(child)
                if child_hash:
                    children_hashes.append(child_hash)

            node_id, _ = self._create_node('tag', name=element.name, attributes=element.attrs, children_hashes=tuple(children_hashes))
            self._create_edge(source_id=parent_id, target_id=node_id, relationship='CONTAINS_TAG', order=order)

            for child_order, child in enumerate(element.children, start=1):
                self._process_node(child, parent_id=node_id, order=child_order)
        else:
            logger.warning(f"Unknown element type: {type(element)}")
            return '', ''

        return node_id, _

    def _create_node(self, node_type: str, **kwargs) -> Tuple[str, str]:
        identifier = (node_type, kwargs.get('name', ''), make_hashable(kwargs.get('attributes')), kwargs.get('children_hashes', ()), kwargs.get('content', ''))
        
        with self.cache_lock:
            existing_node_id = self.subtree_node_cache.get(identifier)
            if existing_node_id:
                subtree_hash = self.subtree_hash_cache.get(identifier)
                return existing_node_id, subtree_hash

        subtree_hash = self._hash_subtree(**kwargs, node_type=node_type)

        node_id = f"n{self.node_id_counter}"
        self.node_id_counter += 1

        node = {'id': node_id, 'type': node_type}

        if node_type == 'textnode':
            node['data_id'] = self._create_text(kwargs['content'])
        elif node_type == 'doctype':
            node['data_id'] = self._create_doctype(kwargs['content'])
        elif node_type == 'comment':
            node['data_id'] = self._create_comment(kwargs['content'])
        elif node_type == 'tag':
            node['name'] = kwargs['name']
            if kwargs.get('attributes'):
                attribute_id = self._create_attribute(kwargs['attributes'])
                node['attributes_id'] = attribute_id
        elif node_type == 'document':
            node['filename'] = kwargs['filename']

        self.structure['nodes'].append(node)
        self.subtree_node_cache[identifier] = node_id

        return node_id, subtree_hash

    def _create_edge(self, source_id: str, target_id: str, relationship: str, order: int = None):
        edge_key = (source_id, target_id, relationship, order)
        if edge_key in self.edge_set:
            return
        self.edge_set.add(edge_key)

        edge = {
            'source': source_id,
            'target': target_id,
            'relationship': relationship,
            'order': order
        }
        self.structure['edges'].append(edge)

    def _hash_subtree(self, **kwargs) -> str:
        node_type = kwargs['node_type']
        name = kwargs.get('name', '')
        attributes = kwargs.get('attributes', None)
        children_hashes = kwargs.get('children_hashes', ())
        content = kwargs.get('content', '')

        identifier = (node_type, name, make_hashable(attributes), children_hashes, content)
        
        with self.cache_lock:
            cached_hash = self.subtree_hash_cache.get(identifier)
            if cached_hash:
                return cached_hash

        hasher = xxhash.xxh64()
        hasher.update(node_type.encode('utf-8'))
        if name:
            hasher.update(name.encode('utf-8'))
        if attributes:
            hasher.update(str(attributes).encode('utf-8'))
        if content:
            hasher.update(content.encode('utf-8'))
        for child_hash in children_hashes:
            hasher.update(child_hash.encode('utf-8'))
        hash_result = hasher.hexdigest()

        with self.cache_lock:
            self.subtree_hash_cache[identifier] = hash_result

        return hash_result

    def _create_text(self, content: str) -> str:
        with self.cache_lock:
            text_id = self.text_lookup.get(content)
            if text_id:
                return text_id
            text_id = f"t{len(self.data['texts']) + 1}"
            self.data['texts'][text_id] = content
            self.text_lookup[content] = text_id
            return text_id

    def _create_doctype(self, content: str) -> str:
        with self.cache_lock:
            doctype_id = self.doctype_lookup.get(content)
            if doctype_id:
                return doctype_id
            doctype_id = f"d{len(self.data['doctypes']) + 1}"
            self.data['doctypes'][doctype_id] = content
            self.doctype_lookup[content] = doctype_id
            return doctype_id

    def _create_comment(self, content: str) -> str:
        with self.cache_lock:
            comment_id = self.comment_lookup.get(content)
            if comment_id:
                return comment_id
            comment_id = f"c{len(self.data['comments']) + 1}"
            self.data['comments'][comment_id] = content
            self.comment_lookup[content] = comment_id
            return comment_id

    def _create_attribute(self, attributes: Dict[str, Any]) -> str:
        attributes_key = make_hashable(attributes)
        with self.cache_lock:
            attribute_id = self.attribute_lookup.get(attributes_key)
            if attribute_id:
                return attribute_id
            attribute_id = f"a{self.attribute_id_counter}"
            self.attribute_id_counter += 1
            self.data['attributes'][attribute_id] = attributes
            self.attribute_lookup[attributes_key] = attribute_id
            return attribute_id

    def _compute_subtree_hash(self, element: Any) -> Tuple[str, str]:
        if isinstance(element, Doctype):
            hash_value = self._hash_subtree(node_type='doctype', content=str(element))
        elif isinstance(element, Comment):
            hash_value = self._hash_subtree(node_type='comment', content=str(element))
        elif isinstance(element, NavigableString):
            if element.strip():
                hash_value = self._hash_subtree(node_type='textnode', content=element.strip())
            else:
                return '', ''
        elif isinstance(element, Tag):
            children_hashes = []
            for child in element.children:
                _, child_hash = self._compute_subtree_hash(child)
                if child_hash:
                    children_hashes.append(child_hash)
            hash_value = self._hash_subtree(node_type='tag', name=element.name, attributes=element.attrs, children_hashes=tuple(children_hashes))
        else:
            logger.warning(f"Unknown element type for hashing: {type(element)}")
            return '', ''
        return '', hash_value

    async def save_results(self, output_dir: Path):
        data_yaml_path = output_dir / config.files.data_yaml
        structure_yaml_path = output_dir / config.files.structure_yaml
        data_pickle_path = output_dir / config.files.data_pickle
        structure_pickle_path = output_dir / config.files.structure_pickle

        await asyncio.gather(
            FileOperations.save_yaml(self.data, data_yaml_path),
            FileOperations.save_yaml(self.structure, structure_yaml_path),
            FileOperations.save_pickle(self.data, data_pickle_path),
            FileOperations.save_pickle(self.structure, structure_pickle_path)
        )

        logger.info("Decomposed data and structure saved successfully.")

async def main():
    output_dir = config.paths.decomposed_output_dir
    
    if output_dir.exists() and any(output_dir.iterdir()):
        logger.info("Output directory already exists and contains files. Skipping decomposition process.")
        return

    await FileOperations.ensure_directory(output_dir)

    decomposer = Decomposer()
    await decomposer.process()
    await decomposer.save_results(output_dir)

    logger.info("Decomposition process completed successfully.")

if __name__ == "__main__":
    asyncio.run(main())