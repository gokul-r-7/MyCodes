# SMB Helper classes
import os
import re
from smbclient import listdir, mkdir, register_session, rmdir, scandir , remove,delete_session
from smbclient.shutil import copyfile,open_file
from smbclient import open_file
from datetime import datetime
from .logger import get_logger
import smbclient
import time
import posixpath
import time
import fnmatch

# Init logger
logger = get_logger(__name__)



class SMBConnection:
    def __init__(self, hostname, username, password, port=445):
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.smb = None

    def __enter__(self):
        # Register the session with the SMB server
        self.smb = register_session(self.hostname, self.username, self.password)
        logger.info("connected successfully")
        return self.smb

    def __exit__(self,hostname, username, password):
        if self.smb:
            delete_session(self.hostname)
            logger.info("SMB session closed")

class SMBFileListor:
    def __init__(self, hostname, username, password, remote_folder, file_pattern=None):
        self.pathprefix = f"//{hostname}"
        self.remote_folder = remote_folder
        self.hostname = hostname
        self.username = username
        self.password = password
        self.file_pattern = file_pattern  # This should be a regex pattern

        # Authenticate with SMB
        smbclient.ClientConfig(username=username, password=password)

    def list_files(self):
        try:
            # Build the full path for the SMB directory
            remote_path = self.pathprefix + self.remote_folder
            logger.info(f"Listing files in SMB directory: {remote_path}")
            files = []

            # Get all entries in the directory
            all_entries = listdir(remote_path)

            logger.info(f"All the available files in remote location - {all_entries}")

            # Filter out directories
            files = [entry for entry in all_entries if not entry.endswith('/')]


            if self.file_pattern:
                # Below Code Working for Regex
                #pattern = re.compile(self.file_pattern)
                #files = [f for f in files if pattern.search(f)]
                files = fnmatch.filter(files, self.file_pattern)
            logger.info(f"Filtered files - {files}")
            return files
        except Exception as e:
            logger.error(f"Error while listing files: {e}")
            raise e
        
    def list_files_recursive(self):
        """Recursively lists files in SMB path that match the file pattern (regex)."""
        remote_path = self.pathprefix + self.remote_folder
        logger.info(f"Starting recursive file listing from: {remote_path}")
        
        matched_files = self._list_files_recursive(remote_path)
        
        logger.info(f"Total matched files: {len(matched_files)} -> {matched_files}")
        return matched_files

    def _list_files_recursive(self, path):
        """Helper function to list files recursively in SMB."""
        try:
            logger.debug(f"Checking SMB directory: {path}")
            entries = smbclient.listdir(path)
            logger.debug(f"Found {len(entries)} entries in {path}: {entries}")

            matched_files = []

            for entry in entries:
                full_path = os.path.join(path, entry)
                logger.debug(f"Processing entry: {entry}, full path: {full_path}")

                # Check if the entry is a directory
                try:
                    stat_info = smbclient.stat(full_path)
                    if stat_info.st_mode & 0o170000 == 0o040000:
                        logger.debug(f"Found directory: {full_path} - Recursing into it.")
                        matched_files.extend(self._list_files_recursive(full_path))
                    else:
                        logger.debug(f"Found file: {full_path}")
                        # Ensure file_pattern is set
                        if not self.file_pattern:
                            logger.info(f"No pattern provided. Adding file: {full_path}")
                            matched_files.append(full_path)
                        else:
                            logger.debug(f"Applying regex pattern: {self.file_pattern} to file: {entry}")
                            if re.match(self.file_pattern, entry):
                                logger.info(f"File matches pattern: {full_path}")
                                matched_files.append(full_path)
                            else:
                                logger.debug(f"File does not match pattern: {entry}")
                except Exception as stat_error:
                    logger.error(f"Error accessing {full_path}: {stat_error}")

            return matched_files
        except Exception as e:
            logger.error(f"Error while listing files in {path}: {e}")
            return []
        
class SMBUploader:
    def __init__(self, hostname,remote_folder,username, password):
        self.pathprefix=r"//" + hostname
        self.remote_folder = remote_folder
        self.hostname = hostname
        self.username = username
        self.password = password

    def upload_files(self, files):
        uploaded_files = []

        logger.info("Current Working Directory")
        logger.info(os.system("pwd"))
        logger.info(os.system('ls -la'))
        
        try:
            for file in files:
                remote_path = os.path.join(self.remote_folder, os.path.basename(file))
                remote_path = f"{self.pathprefix}{remote_path}"
                logger.info(f"Uploading: {file} to {remote_path}")
                copyfile(file, remote_path,username=self.username, password=self.password)
                logger.info(f"Uploaded: {file}")
                uploaded_files.append(remote_path)
            return uploaded_files
        except Exception as e:
                logger.error(f"Error uploading {file}: {str(e)}")
                raise e
        
class SMBDownloader:
    def __init__(self, hostname, username, password, remote_folder, buffer_size=1024 * 1024):
        """
        Initializes the SMBDownloader instance.

        :param hostname: The hostname or IP address of the SMB server
        :param username: Username for SMB authentication
        :param password: Password for SMB authentication
        :param remote_folder: The remote folder path to download files from
        :param buffer_size: The buffer size (default: 64KB)
        """
        self.pathprefix = r"//" + hostname
        self.remote_folder = remote_folder
        self.hostname = hostname
        self.username = username
        self.password = password
        self.buffer_size = buffer_size  # Default buffer size is 64KB

    def download_files(self, files):
        try:
            downloaded_files = []
            # Extract the last part of the remote folder path to use as the local folder name
            local_folder_name = os.path.basename(self.remote_folder)
            local_base_path = os.path.join('/tmp', local_folder_name)

            # Create the local directory if it doesn't exist
            os.makedirs(local_base_path, exist_ok=True)

            # Build the full path for the SMB directory
            smb_base_path = self.pathprefix + self.remote_folder

            for file in files:
                logger.info(f"Commence download for {file}")
                start_time = time.time()  # Start the timer
                
                local_path = os.path.join(local_base_path, file)
                remote_path = os.path.join(smb_base_path, file)

                logger.info(f"Downloading from {remote_path} to {local_path}")

                # Open the remote SMB file and read in chunks
                with open_file(remote_path, username=self.username, password=self.password,mode='rb',buffering=self.buffer_size,share_access='r') as remote_file:
                    with open(local_path, 'wb') as local_file:
                        # Read the remote file in chunks and write to the local file
                        while True:
                            buffer = remote_file.read(self.buffer_size)
                            if not buffer:  # EOF reached
                                break
                            local_file.write(buffer)

                # Log download completion and time taken
                end_time = time.time()  # End the timer
                logger.info(f"Downloaded: {file} in {end_time - start_time:.2f} seconds")

                downloaded_files.append(local_path)

            return downloaded_files

        except Exception as e:
            logger.error(f"Error while downloading files: {e}")
            raise e

    def download_bot_files(self, files):
        """
        Downloads the given files from the SMB server while preserving folder structure.

        :param files: List of SMB file paths to download.
        :return: List of downloaded local file paths.
        """
        try:
            downloaded_files = []

            # Extract folder name for local storage
            local_base_path = os.path.join('/tmp', os.path.basename(self.remote_folder))

            for remote_file in files:
                logger.info(f"Commencing download for {remote_file}")

                start_time = time.time()  # Start timer

                # Extract relative path to maintain subdirectory structure
                relative_path = os.path.relpath(remote_file, self.pathprefix + self.remote_folder)
                local_path = os.path.join(local_base_path, relative_path)

                # Ensure local directories exist
                os.makedirs(os.path.dirname(local_path), exist_ok=True)

                logger.info(f"Downloading from {remote_file} to {local_path}")

                # Open remote SMB file and read in chunks
                with open_file(remote_file, username=self.username, password=self.password, mode='rb',
                               buffering=self.buffer_size, share_access='r') as remote_file_obj:
                    with open(local_path, 'wb') as local_file_obj:
                        while True:
                            buffer = remote_file_obj.read(self.buffer_size)
                            if not buffer:  # EOF
                                break
                            local_file_obj.write(buffer)

                end_time = time.time()  # End timer
                logger.info(f"Downloaded: {local_path} in {end_time - start_time:.2f} seconds")

                downloaded_files.append(local_path)

            return downloaded_files

        except Exception as e:
            logger.error(f"Error while downloading files: {e}")
            raise e
        
class SMBArchiveHandler:
    def __init__(self, remote_folder, archive_folder, hostname, username, password):
        self.pathprefix = r"//" + hostname
        self.remote_folder = remote_folder
        self.archive_folder = archive_folder
        self.hostname = hostname
        self.username = username
        self.password = password
        
        # Build the full path for the SMB directory
        self.remote_folder = self.pathprefix + self.remote_folder
        self.archive_folder = self.pathprefix + self.archive_folder
    def archive_and_delete_file(self, file_name, file_archive_flag=True, file_delete_flag=True):
        try:
            base_file_name = os.path.basename(file_name)

            if file_archive_flag is True or str(file_archive_flag).lower() == "true":
                # Add timestamp to the file name
                timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

                archived_file_name = f"{base_file_name}_{timestamp}"
                archive_path = os.path.join(self.archive_folder, archived_file_name)

                # Archive the file by copying to the archive folder
                source_path = os.path.join(self.remote_folder, file_name)
                copyfile(source_path, archive_path)
                logger.info(f"Archived {file_name} to {archive_path}")
                
            if file_delete_flag is True or str(file_delete_flag).lower() == "true":
                # Delete the file from the remote folder
                file_path = os.path.join(self.remote_folder, file_name)
                remove(file_path)
                logger.info(f"Deleted {file_name} from SMB location {self.remote_folder}")
                
        except Exception as e:
            logger.error(f"Error while archiving and deleting file: {e}")
            raise e