import os
import hashlib
import asyncio
import aiofiles
import aiosqlite
import uvloop
import shutil
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import Dict, Tuple, List, Optional, Set

from bs4 import BeautifulSoup
from tqdm.asyncio import tqdm
from urllib.parse import urlparse, quote, unquote, unquote_plus
import logging
import rapidfuzz.fuzz

from config.config import config
from src.utils.logging import Logger
from src.utils.file_operations import FileOperations

# Set up logging
logging.basicConfig(
    level=logging.INFO,  # Change to DEBUG for detailed logs during development
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
INPUT_DIR = config.paths.input_folder
OUTPUT_DIR = config.paths.manual_cleaned_html_data
DB_NAME = config.database.consolidated_html_db
CRAWLER_DB = config.database.crawler_db
BATCH_SIZE = 100  # Adjust based on your system's capabilities
SIMILARITY_THRESHOLD = 99.0  # 99% similarity

# Database Schema
DATABASE_SCHEMA = '''
CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY,
    file_name TEXT UNIQUE,
    title TEXT,
    url TEXT,
    relative_url TEXT,
    form_action TEXT,
    form_action_decoded TEXT
);

CREATE TABLE IF NOT EXISTS canonical_mapping (
    canonical_file TEXT,
    duplicate_file TEXT,
    canonical_url TEXT,
    duplicate_url TEXT,
    PRIMARY KEY (canonical_file, duplicate_file),
    FOREIGN KEY (canonical_file) REFERENCES files (file_name)
);

CREATE TABLE IF NOT EXISTS meta_tags (
    id INTEGER PRIMARY KEY,
    file_id INTEGER,
    name TEXT,
    content TEXT,
    FOREIGN KEY (file_id) REFERENCES files (id)
);

CREATE TABLE IF NOT EXISTS external_links (
    id INTEGER PRIMARY KEY,
    file_id INTEGER,
    raw_element TEXT,
    label TEXT,
    cleaned_label TEXT,
    raw_url TEXT,
    url_end TEXT,
    clean_url TEXT,
    is_external_link BOOLEAN,
    FOREIGN KEY (file_id) REFERENCES files (id)
);

CREATE INDEX IF NOT EXISTS idx_meta_tags_file_id ON meta_tags(file_id);
CREATE INDEX IF NOT EXISTS idx_external_links_file_id ON external_links(file_id);
'''

@asynccontextmanager
async def managed_directory(directory: str):
    """
    Ensure the directory exists.
    """
    try:
        os.makedirs(directory, exist_ok=True)
        yield directory
    except Exception as e:
        logger.error(f"Error occurred while working with directory {directory}: {e}")
        raise

@asynccontextmanager
async def managed_database(db_name: str):
    """
    Manage the SQLite database connection.
    """
    try:
        async with aiosqlite.connect(db_name) as conn:
            await conn.execute("PRAGMA journal_mode = WAL")
            await conn.execute("PRAGMA synchronous = NORMAL")
            await conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            yield conn
    except Exception as e:
        logger.error(f"Error managing database {db_name}: {e}")
        raise

async def init_database(conn: aiosqlite.Connection):
    """
    Initialize the database with required tables.
    """
    try:
        await conn.executescript(DATABASE_SCHEMA)
        await conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise

def get_url_hash(url: str) -> str:
    """
    Generate MD5 hash for a given URL.
    """
    return hashlib.md5(url.encode()).hexdigest()

async def compute_file_hash(filename: str, directory: str) -> str:
    """
    Compute MD5 hash of the file's contents.
    """
    file_path = os.path.join(directory, filename)
    try:
        async with aiofiles.open(file_path, 'rb') as file:
            content = await file.read()
        return hashlib.md5(content).hexdigest()
    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        return ''
    except Exception as e:
        logger.error(f"Error computing hash for {file_path}: {e}")
        return ''

async def process_file(filename: str, input_dir: str, url_mapping: Dict[str, str]) -> Tuple[str, str, str, str]:
    """
    Process a single file to compute its hash and extract URL information.
    """
    try:
        file_hash = await compute_file_hash(filename, input_dir)
        
        # Use the full filename (including .html) as the key for url_mapping
        url = url_mapping.get(filename, '')
        
        if not url:
            logger.warning(f"No URL found for file: {filename}")
            return filename, file_hash, '', ''
        
        # Parse the URL
        parsed_url = urlparse(url)
        
        # Construct relative URL
        relative_url = parsed_url.path
        if parsed_url.query:
            relative_url += f"?{parsed_url.query}"
        if parsed_url.fragment:
            relative_url += f"#{parsed_url.fragment}"
        
        # Remove leading slash if present
        relative_url = relative_url.lstrip('/')
        
        return filename, file_hash, url, relative_url
    
    except Exception as e:
        logger.error(f"Error processing file {filename}: {e}")
        return filename, '', '', ''

class UnionFind:
    def __init__(self):
        self.parent = {}
    
    def find(self, item):
        if item not in self.parent:
            self.parent[item] = item
        if self.parent[item] != item:
            self.parent[item] = self.find(self.parent[item])
        return self.parent[item]
    
    def union(self, a, b):
        root_a = self.find(a)
        root_b = self.find(b)
        if root_a != root_b:
            self.parent[root_b] = root_a

async def find_duplicates(input_dir: str, url_mapping: Dict[str, str], uf: UnionFind) -> Tuple[Dict[str, Tuple[str, str]], Dict[str, List[Tuple[str, str]]]]:
    """
    Identify duplicate files based on file hash and relative URL, updating the Union-Find structure.
    """
    try:
        files = [f for f in os.listdir(input_dir) if f.endswith('.html')]
        file_hashes: Dict[str, Tuple[str, str]] = {}
        relative_url_mapping: defaultdict = defaultdict(list)

        tasks = [process_file(f, input_dir, url_mapping) for f in files]
        results = await tqdm.gather(*tasks, desc="Processing files")

        for filename, file_hash, url, relative_url in results:
            if file_hash:
                if file_hash in file_hashes:
                    # Exact duplicate found; union the two files
                    uf.union(file_hashes[file_hash][0], filename)
                else:
                    file_hashes[file_hash] = (filename, url)
            
            if relative_url:
                relative_url_mapping[relative_url].append((filename, url))

        # Find duplicates based on relative URL
        for relative_url, file_list in relative_url_mapping.items():
            if len(file_list) > 1:
                canonical_file = file_list[0][0]
                for dup_file, _ in file_list[1:]:
                    uf.union(canonical_file, dup_file)

        logger.info(f"Found {len(file_hashes)} unique files based on hashes and relative URLs.")
        return file_hashes, relative_url_mapping
    except Exception as e:
        logger.error(f"Error finding duplicates: {e}")
        raise

async def find_additional_duplicates(input_dir: str, relative_url_mapping: Dict[str, List[Tuple[str, str]]], uf: UnionFind, file_hashes: Dict[str, Tuple[str, str]], url_mapping: Dict[str, str]) -> List[Tuple[str, str]]:
    """
    Perform a second deduplication pass based on similarity for files with matching relative URLs.
    Returns a list of duplicate pairs to be unioned.
    """
    try:
        # List to store duplicate pairs
        duplicate_pairs: List[Tuple[str, str]] = []

        # Function to load file content asynchronously
        async def load_content(file_name: str) -> Tuple[str, str]:
            file_path = os.path.join(input_dir, file_name)
            try:
                async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = await f.read()
                return file_name, content
            except Exception as e:
                logger.error(f"Error reading file {file_path}: {e}")
                return file_name, ""

        # Collect all necessary files (only duplicates from relative_url_mapping)
        tasks = []
        for relative_url, files in relative_url_mapping.items():
            if len(files) > 1:
                # Collect pairs where files have different hashes
                # Assuming that if they are already in UF, they have been processed
                canonical_file = files[0][0]
                for dup_file, _ in files[1:]:
                    if uf.find(canonical_file) != uf.find(dup_file):
                        tasks.append(load_content(canonical_file))
                        tasks.append(load_content(dup_file))

        logger.info(f"Loading contents of {len(tasks)} files for similarity comparison...")
        file_contents_list = await tqdm.gather(*tasks, desc="Loading file contents")
        file_contents = {fname: content for fname, content in file_contents_list if content}

        # Define a helper function for similarity comparison
        async def compare_similarity(canonical_file: str, duplicate_file: str) -> Optional[Tuple[str, str]]:
            """
            Compare the similarity between two files' contents.
            """
            try:
                canonical_content = file_contents.get(canonical_file, "")
                duplicate_content = file_contents.get(duplicate_file, "")
                if not canonical_content or not duplicate_content:
                    return None

                # Run the similarity comparison in a separate thread to avoid blocking
                similarity = await asyncio.to_thread(rapidfuzz.fuzz.ratio, canonical_content, duplicate_content)
                if similarity >= SIMILARITY_THRESHOLD:
                    logger.debug(f"Duplicate found: {duplicate_file} is {similarity}% similar to {canonical_file}")
                    return (canonical_file, duplicate_file)
                return None
            except Exception as e:
                logger.error(f"Error comparing {canonical_file} and {duplicate_file}: {e}")
                return None

        logger.info("Starting similarity comparisons...")
        comparison_tasks = []
        # Reconstruct the list of pairs to compare
        for relative_url, files in relative_url_mapping.items():
            if len(files) > 1:
                canonical_file = files[0][0]
                for dup_file, _ in files[1:]:
                    if uf.find(canonical_file) != uf.find(dup_file):
                        comparison_tasks.append(compare_similarity(canonical_file, dup_file))

        comparison_results = await tqdm.gather(*comparison_tasks, desc="Comparing file similarities")

        for result in comparison_results:
            if result:
                duplicate_pairs.append(result)

        # Union the duplicate pairs
        for canonical, duplicate in duplicate_pairs:
            uf.union(canonical, duplicate)

        logger.info(f"Identified {len(duplicate_pairs)} additional duplicate pairs based on similarity.")
        return duplicate_pairs
    except Exception as e:
        logger.error(f"Error during additional deduplication: {e}")
        raise

async def parse_html_file(file_path: str, url_mapping: Dict[str, str]) -> Optional[Dict]:
    """
    Parse an HTML file and extract relevant data.
    """
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
            html_content = await file.read()

        soup = BeautifulSoup(html_content, 'lxml')
        
        title = soup.title.string.strip() if soup.title and soup.title.string else ""
        
        file_name = os.path.basename(file_path)
        url = url_mapping.get(file_name, '')
        _, _, _, relative_url = await process_file(file_name, os.path.dirname(file_path), url_mapping)
        
        form = soup.find('form')
        form_action = form.get('action') if form else ""
        form_action_decoded = unquote_plus(form_action).replace('./', '') if form_action else None
        
        meta_tags = []
        for meta in soup.find_all('meta'):
            name = meta.get('name') or meta.get('property')
            content = meta.get('content')
            if name and content:
                meta_tags.append((name, content))
        
        external_links = []
        for link in soup.find_all('a', href=True):
            href = link.get('href', '')
            if 'paizo.com' in href:
                raw_element = str(link)
                label = link.get_text(strip=True)
                cleaned_label = label.split(" pg.")[0] if " pg." in label else label
                url_end = href.split('/')[-1] if '/' in href else ''
                clean_url = f"https://paizo.com/products/{url_end}" if url_end else href
                is_external_link = 'external-link' in (link.get('class') or [])
                external_links.append((raw_element, label, cleaned_label, href, url_end, clean_url, is_external_link))
        
        return {
            'file_name': file_name,
            'title': title,
            'url': url,
            'relative_url': relative_url,
            'form_action': form_action,
            'form_action_decoded': form_action_decoded,
            'meta_tags': meta_tags,
            'external_links': external_links
        }
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return None

async def process_html_files(input_dir: str, url_mapping: Dict[str, str], unique_files: Set[str]) -> List[Dict]:
    """
    Process multiple HTML files asynchronously.
    """
    try:
        async def process(file_name: str) -> Optional[Dict]:
            file_path = os.path.join(input_dir, file_name)
            return await parse_html_file(file_path, url_mapping)
        
        tasks = [process(f) for f in unique_files]
        results = []
        for i in tqdm(range(0, len(tasks), BATCH_SIZE), desc="Processing file batches"):
            batch = tasks[i:i+BATCH_SIZE]
            batch_results = await asyncio.gather(*batch)
            results.extend([r for r in batch_results if r is not None])
        return results
    except Exception as e:
        logger.error(f"Error processing HTML files: {e}")
        raise

async def insert_data(conn: aiosqlite.Connection, processed_files: List[Dict], uf: UnionFind, file_hashes: Dict[str, Tuple[str, str]], url_mapping: Dict[str, str]):
    """
    Insert processed data into the database.
    """
    try:
        async with conn.cursor() as cursor:
            # Insert into files table
            for i in tqdm(range(0, len(processed_files), BATCH_SIZE), desc="Inserting files"):
                batch = processed_files[i:i+BATCH_SIZE]
                await cursor.executemany('''
                    INSERT OR IGNORE INTO files 
                    (file_name, title, url, relative_url, form_action, form_action_decoded)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', [
                    (
                        file_data['file_name'],
                        file_data['title'],
                        file_data['url'],
                        file_data['relative_url'],
                        file_data['form_action'],
                        file_data['form_action_decoded']
                    ) for file_data in batch
                ])
            
            await conn.commit()

            # Retrieve file IDs
            file_id_map = {}
            file_names = [file['file_name'] for file in processed_files]
            # Split into chunks to avoid SQLite parameter limit
            for i in range(0, len(file_names), 999):  # SQLite has a limit of 999 variables per query
                chunk = file_names[i:i+999]
                placeholders = ','.join(['?']*len(chunk))
                query = f"SELECT id, file_name FROM files WHERE file_name IN ({placeholders})"
                async with conn.execute(query, chunk) as select_cursor:
                    async for row in select_cursor:
                        file_id_map[row[1]] = row[0]
            
            # Insert into meta_tags table
            for i in tqdm(range(0, len(processed_files), BATCH_SIZE), desc="Inserting meta tags"):
                batch = processed_files[i:i+BATCH_SIZE]
                meta_entries = []
                for file_data in batch:
                    file_id = file_id_map.get(file_data['file_name'])
                    if file_id:
                        meta_entries.extend([
                            (file_id, name, content) for name, content in file_data['meta_tags']
                        ])
                if meta_entries:
                    await cursor.executemany('''
                        INSERT OR IGNORE INTO meta_tags (file_id, name, content)
                        VALUES (?, ?, ?)
                    ''', meta_entries)
            
            await conn.commit()

            # Insert into external_links table
            for i in tqdm(range(0, len(processed_files), BATCH_SIZE), desc="Inserting external links"):
                batch = processed_files[i:i+BATCH_SIZE]
                link_entries = []
                for file_data in batch:
                    file_id = file_id_map.get(file_data['file_name'])
                    if file_id:
                        link_entries.extend([
                            (file_id, *link) for link in file_data['external_links']
                        ])
                if link_entries:
                    await cursor.executemany('''
                        INSERT OR IGNORE INTO external_links 
                        (file_id, raw_element, label, cleaned_label, raw_url, url_end, clean_url, is_external_link)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', link_entries)
            
            await conn.commit()

            # Prepare canonical_mapping entries from Union-Find
            logger.info("Preparing canonical mapping entries...")
            group_map = defaultdict(list)
            for file_hash, (file_name, _) in file_hashes.items():
                root = uf.find(file_name)
                group_map[root].append(file_name)

            canonical_entries = []
            for group in group_map.values():
                if len(group) > 1:
                    canonical_file = group[0]
                    canonical_url = url_mapping.get(canonical_file, '')
                    for duplicate_file in group[1:]:
                        duplicate_url = url_mapping.get(duplicate_file, '')
                        canonical_entries.append((canonical_file, duplicate_file, canonical_url, duplicate_url))

            # Insert into canonical_mapping table
            if canonical_entries:
                for i in tqdm(range(0, len(canonical_entries), BATCH_SIZE), desc="Inserting canonical mappings"):
                    batch = canonical_entries[i:i+BATCH_SIZE]
                    await cursor.executemany('''
                        INSERT OR IGNORE INTO canonical_mapping 
                        (canonical_file, duplicate_file, canonical_url, duplicate_url)
                        VALUES (?, ?, ?, ?)
                    ''', batch)
                await conn.commit()
        
        logger.info("Data insertion completed successfully.")
    except Exception as e:
        logger.error(f"Error inserting data into database: {e}")
        raise

async def save_deduplicated_files(input_dir: str, output_dir: str, unique_files: Set[str]):
    """
    Save unique files to the output directory.
    """
    try:
        for file_name in tqdm(unique_files, desc="Saving deduplicated files"):
            src = os.path.join(input_dir, file_name)
            dst = os.path.join(output_dir, file_name)
            if not os.path.exists(dst):  # Check if the file already exists
                try:
                    shutil.copy2(src, dst)
                except FileNotFoundError:
                    logger.error(f"File not found for copying: {src}")
                except Exception as e:
                    logger.error(f"Error copying {src} to {dst}: {e}")
    except Exception as e:
        logger.error(f"Error saving deduplicated files: {e}")
        raise

async def load_url_mapping(crawler_db_path: str) -> Dict[str, str]:
    """
    Load URL mappings from the crawler database by computing MD5 hash of each URL as the file name.
    """
    try:
        url_mapping = {}
        async with aiosqlite.connect(crawler_db_path) as conn:
            await conn.execute("PRAGMA journal_mode = WAL")
            await conn.execute("PRAGMA synchronous = NORMAL")
            await conn.execute("PRAGMA cache_size = -64000")  # 64MB cache
            async with conn.execute('SELECT url FROM urls WHERE status = "completed"') as cursor:
                async for row in cursor:
                    url = row[0]
                    file_hash = get_url_hash(url)
                    file_name = f"{file_hash}.html"  # Assuming file names have .html extension
                    url_mapping[file_name] = url
        return url_mapping
    except Exception as e:
        logger.error(f"Error loading URL mappings: {e}")
        raise

async def determine_unique_files(uf: UnionFind, file_hashes: Dict[str, Tuple[str, str]]) -> Set[str]:
    """
    Determine the set of unique file names based on the Union-Find structure.
    """
    try:
        all_duplicates: Set[str] = set()
        group_map = defaultdict(list)
        for file_hash, (file_name, _) in file_hashes.items():
            root = uf.find(file_name)
            group_map[root].append(file_name)
        for group in group_map.values():
            if len(group) > 1:
                # Exclude the first file as it is the canonical one
                all_duplicates.update(group[1:])
        unique_files: Set[str] = set(file_hashes[f_hash][0] for f_hash in file_hashes if file_hashes[f_hash][0] not in all_duplicates)
        return unique_files
    except Exception as e:
        logger.error(f"Error determining unique files: {e}")
        raise

async def main():
    """
    Main entry point of the script.
    """
    try:
        # Install uvloop for better performance
        uvloop.install()
        # Initialize Union-Find structure
        uf = UnionFind()
        # Ensure the output directory exists and manage the database connection
        async with managed_directory(OUTPUT_DIR) as output_dir, managed_database(DB_NAME) as conn:
            logger.info("Initializing database...")
            await init_database(conn)
            logger.info("Loading URL mappings...")
            url_mapping = await load_url_mapping(CRAWLER_DB)
            logger.info(f"Loaded {len(url_mapping)} URL mappings")
            logger.info("Finding duplicates and relative URL matches...")
            file_hashes, relative_url_mapping = await find_duplicates(INPUT_DIR, url_mapping, uf)
            logger.info(f"Found {len(file_hashes)} unique files based on hashes and relative URLs.")
            logger.info("Performing second deduplication pass based on similarity...")
            duplicate_pairs = await find_additional_duplicates(INPUT_DIR, relative_url_mapping, uf, file_hashes, url_mapping)
            logger.info("Second deduplication pass completed.")
            # Determine unique files
            unique_files = await determine_unique_files(uf, file_hashes)
            logger.info(f"Found {len(unique_files)} unique files after deduplication.")
            logger.info("Processing files...")
            processed_files = await process_html_files(INPUT_DIR, url_mapping, unique_files)
            logger.info(f"Processed {len(processed_files)} files")
            logger.info("Inserting data into database...")
            await insert_data(conn, processed_files, uf, file_hashes, url_mapping)
            logger.info("Saving deduplicated files...")
            await save_deduplicated_files(INPUT_DIR, output_dir, unique_files)
            logger.info("Processing completed successfully")
    except Exception as e:
        logger.error(f"An error occurred during processing: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())