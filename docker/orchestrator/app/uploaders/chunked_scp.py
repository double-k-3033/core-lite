from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from app.models import UploadResult
from app.uploaders.scp import ScpUploader

logger = logging.getLogger(__name__)

# Default chunk size: 512 MB
DEFAULT_CHUNK_SIZE_MB = 512
# Default timeout per chunk: 10 minutes
DEFAULT_CHUNK_TIMEOUT = 600
# Default parallel uploads
DEFAULT_PARALLEL_CHUNKS = 2
# Minimum file size to trigger chunking: 2 GB
DEFAULT_MIN_CHUNK_SIZE_GB = 2
# Buffer size for file I/O: 1 MB
BUFFER_SIZE = 1024 * 1024


@dataclass
class ChunkEntry:
    """Represents a single chunk of a file."""

    index: int
    filename: str
    size: int
    checksum: str
    uploaded: bool = False

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> ChunkEntry:
        return cls(**data)


@dataclass
class ChunkManifest:
    """Tracks chunked upload progress."""

    epoch: int
    tick: int
    archive_name: str
    total_size: int
    chunk_size: int
    checksum: str
    status: str  # "chunking", "uploading", "assembling", "complete", "failed"
    chunks: list[ChunkEntry] = field(default_factory=list)
    created_at: str = ""
    node_id: str = ""

    def to_json(self) -> str:
        data = {
            "epoch": self.epoch,
            "tick": self.tick,
            "archive_name": self.archive_name,
            "total_size": self.total_size,
            "chunk_size": self.chunk_size,
            "checksum": self.checksum,
            "status": self.status,
            "chunks": [c.to_dict() for c in self.chunks],
            "created_at": self.created_at,
            "node_id": self.node_id,
        }
        return json.dumps(data, indent=2)

    @classmethod
    def from_json(cls, data: str) -> ChunkManifest:
        d = json.loads(data)
        chunks = [ChunkEntry.from_dict(c) for c in d.get("chunks", [])]
        return cls(
            epoch=d["epoch"],
            tick=d["tick"],
            archive_name=d["archive_name"],
            total_size=d["total_size"],
            chunk_size=d["chunk_size"],
            checksum=d["checksum"],
            status=d["status"],
            chunks=chunks,
            created_at=d.get("created_at", ""),
            node_id=d.get("node_id", ""),
        )

    @property
    def pending_chunks(self) -> list[ChunkEntry]:
        return [c for c in self.chunks if not c.uploaded]

    @property
    def uploaded_count(self) -> int:
        return sum(1 for c in self.chunks if c.uploaded)

    @property
    def uploaded_bytes(self) -> int:
        return sum(c.size for c in self.chunks if c.uploaded)


class ChunkedScpUploader(ScpUploader):
    """Uploads large files in chunks with resume capability.

    For files larger than min_chunk_size_gb, the file is split into
    fixed-size chunks that are uploaded individually. Progress is
    tracked in a manifest stored on the remote server, enabling
    resume after failures.

    After all chunks are uploaded, they are reassembled on the
    remote server using cat.

    Also supports uploading pre-split chunks (from streaming compression)
    via the upload_chunks() method.
    """

    def __init__(
        self,
        host: str,
        user: str = "",
        port: int = 22,
        dest_path: str = "/snapshots",
        key_file: str = "",
        timeout: int = 1800,
        chunk_size_mb: int = DEFAULT_CHUNK_SIZE_MB,
        chunk_timeout: int = DEFAULT_CHUNK_TIMEOUT,
        parallel_chunks: int = DEFAULT_PARALLEL_CHUNKS,
        min_chunk_size_gb: int = DEFAULT_MIN_CHUNK_SIZE_GB,
    ) -> None:
        super().__init__(
            host=host,
            user=user,
            port=port,
            dest_path=dest_path,
            key_file=key_file,
            timeout=timeout,
        )
        self._chunk_size = chunk_size_mb * 1024 * 1024  # Convert to bytes
        self._chunk_size_mb = chunk_size_mb
        self._chunk_timeout = chunk_timeout
        self._parallel_chunks = parallel_chunks
        self._min_chunk_size = min_chunk_size_gb * 1024 * 1024 * 1024  # bytes

    def _chunks_remote_dir(self, epoch: int) -> str:
        """Remote directory for chunk storage."""
        return f"{epoch}/.upload-chunks"

    def _manifest_remote_key(self, epoch: int) -> str:
        """Remote key for the manifest file."""
        return f"{self._chunks_remote_dir(epoch)}/manifest.json"

    async def upload(
        self,
        file_path: Path,
        metadata: dict,
        remote_key: str,
    ) -> UploadResult:
        """Upload a file, using chunking for large files."""
        start = time.monotonic()
        file_size = file_path.stat().st_size

        # For small files, use regular SCP
        if file_size < self._min_chunk_size:
            logger.info(
                f"File size {file_size} bytes < {self._min_chunk_size} bytes, "
                "using regular SCP upload"
            )
            return await super().upload(file_path, metadata, remote_key)

        # Large file - use chunked upload
        logger.info(
            f"File size {file_size} bytes >= {self._min_chunk_size} bytes, "
            f"using chunked upload ({self._chunk_size // (1024*1024)}MB chunks)"
        )

        epoch = metadata.get("epoch", 0)
        tick = metadata.get("tick", 0)

        try:
            result = await self._chunked_upload(
                file_path=file_path,
                epoch=epoch,
                tick=tick,
                remote_key=remote_key,
                metadata=metadata,
            )
            result.duration_seconds = time.monotonic() - start
            return result
        except Exception as e:
            logger.error(f"Chunked upload failed: {e}", exc_info=True)
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    async def upload_chunks(
        self,
        chunk_files: list[Path],
        metadata: dict,
        remote_key: str,
        total_uncompressed_size: int = 0,
    ) -> UploadResult:
        """Upload pre-split chunk files with resume capability.

        This method is used when chunks were created by streaming compression
        (tar | zstd | split) and already exist on disk.

        Args:
            chunk_files: List of chunk file paths in order.
            metadata: Metadata dict with epoch, tick, etc.
            remote_key: Remote path for the final reassembled file.
            total_uncompressed_size: Original uncompressed size (for logging).

        Returns:
            UploadResult indicating success or failure.
        """
        start = time.monotonic()

        if not chunk_files:
            return UploadResult(
                success=False,
                error_message="No chunk files provided",
                duration_seconds=0,
            )

        epoch = metadata.get("epoch", 0)
        tick = metadata.get("tick", 0)

        # Calculate total size
        total_size = sum(c.stat().st_size for c in chunk_files)

        logger.info(
            f"Uploading {len(chunk_files)} pre-split chunks "
            f"({total_size / (1024**3):.2f} GB compressed)"
        )

        try:
            result = await self._upload_presplit_chunks(
                chunk_files=chunk_files,
                epoch=epoch,
                tick=tick,
                remote_key=remote_key,
                total_size=total_size,
            )
            result.duration_seconds = time.monotonic() - start
            return result
        except Exception as e:
            logger.error(f"Pre-split chunk upload failed: {e}", exc_info=True)
            return UploadResult(
                success=False,
                error_message=str(e),
                duration_seconds=time.monotonic() - start,
            )

    async def _upload_presplit_chunks(
        self,
        chunk_files: list[Path],
        epoch: int,
        tick: int,
        remote_key: str,
        total_size: int,
    ) -> UploadResult:
        """Upload pre-split chunks with resume capability."""
        from datetime import datetime, timezone

        staging_dir = chunk_files[0].parent
        # Derive archive name from remote key (e.g., "199/ep199-t123-snap.tar.zst")
        archive_name = Path(remote_key).name

        # Compute combined checksum of all chunks
        logger.info("Computing combined checksum of chunks...")
        combined_checksum = await asyncio.to_thread(
            self._compute_combined_checksum, chunk_files
        )

        # Check for existing manifest (resume case)
        manifest = await self._load_remote_manifest(epoch)
        if manifest and manifest.checksum == combined_checksum:
            logger.info(
                f"Resuming upload: {manifest.uploaded_count}/{len(manifest.chunks)} "
                f"chunks already uploaded"
            )
        else:
            if manifest:
                logger.info(
                    "Chunks changed since last upload attempt, starting fresh"
                )
                await self._cleanup_remote_chunks(epoch)

            # Create manifest for pre-split chunks
            chunks = []
            for i, chunk_path in enumerate(chunk_files):
                chunk_checksum = await asyncio.to_thread(
                    self._compute_file_checksum, chunk_path
                )
                chunks.append(
                    ChunkEntry(
                        index=i,
                        filename=chunk_path.name,
                        size=chunk_path.stat().st_size,
                        checksum=chunk_checksum,
                        uploaded=False,
                    )
                )

            manifest = ChunkManifest(
                epoch=epoch,
                tick=tick,
                archive_name=archive_name,
                total_size=total_size,
                chunk_size=self._chunk_size,
                checksum=combined_checksum,
                status="uploading",
                chunks=chunks,
                created_at=datetime.now(timezone.utc).isoformat(),
            )

        # Ensure remote directory exists
        await self._ensure_remote_chunks_dir(epoch)

        # Save manifest to remote
        manifest.status = "uploading"
        await self._save_remote_manifest(epoch, manifest)

        # Upload pending chunks
        upload_success = await self._upload_pending_chunks(
            manifest=manifest,
            staging_dir=staging_dir,
            epoch=epoch,
        )
        if not upload_success:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Failed to upload all chunks",
            )

        # Reassemble on remote
        logger.info("Reassembling chunks on remote server...")
        manifest.status = "assembling"
        await self._save_remote_manifest(epoch, manifest)

        reassemble_success = await self._reassemble_on_remote(
            manifest=manifest,
            epoch=epoch,
            remote_key=remote_key,
        )
        if not reassemble_success:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Failed to reassemble chunks on remote",
            )

        # Verify size on remote (skip checksum for streaming - it's different)
        logger.info("Verifying remote file size...")
        verify_ok = await self._verify_remote_size(
            remote_key=remote_key,
            expected_size=total_size,
        )
        if not verify_ok:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Remote size verification failed",
            )

        # Cleanup
        logger.info("Cleaning up remote chunks...")
        manifest.status = "complete"
        await self._save_remote_manifest(epoch, manifest)
        await self._cleanup_remote_chunks(epoch)

        # Local chunk cleanup is handled by caller
        logger.info(
            f"Pre-split chunk upload complete: {total_size} bytes in "
            f"{len(manifest.chunks)} chunks"
        )

        return UploadResult(
            success=True,
            remote_url=f"{self._target()}:{self._remote_path(remote_key)}",
            bytes_uploaded=total_size,
        )

    def _compute_combined_checksum(self, chunk_files: list[Path]) -> str:
        """Compute a combined checksum of all chunk files."""
        sha256 = hashlib.sha256()
        for chunk_path in chunk_files:
            with open(chunk_path, "rb") as f:
                while data := f.read(BUFFER_SIZE):
                    sha256.update(data)
        return sha256.hexdigest()

    async def _verify_remote_size(
        self,
        remote_key: str,
        expected_size: int,
    ) -> bool:
        """Verify the size of the reassembled file on remote."""
        remote_path = self._remote_path(remote_key)

        try:
            code, stdout, stderr = await self._run_ssh(
                f"stat -c '%s' {remote_path}", timeout=30
            )
            if code != 0:
                logger.error(
                    f"Remote size check failed: {stderr.strip()}"
                )
                return False

            remote_size = int(stdout.strip())
            if remote_size != expected_size:
                logger.error(
                    f"Size mismatch: expected {expected_size}, "
                    f"got {remote_size}"
                )
                return False

            logger.info(f"Remote size verified: {remote_size} bytes")
            return True

        except Exception as e:
            logger.error(f"Remote size verification error: {e}")
            return False

    async def _chunked_upload(
        self,
        file_path: Path,
        epoch: int,
        tick: int,
        remote_key: str,
        metadata: dict,
    ) -> UploadResult:
        """Perform chunked upload with resume capability."""
        file_size = file_path.stat().st_size
        staging_dir = file_path.parent

        # Step 1: Compute file checksum
        logger.info(f"Computing checksum for {file_path.name}...")
        file_checksum = await asyncio.to_thread(
            self._compute_file_checksum, file_path
        )

        # Step 2: Check for existing manifest (resume case)
        manifest = await self._load_remote_manifest(epoch)
        if manifest and manifest.checksum == file_checksum:
            logger.info(
                f"Resuming upload: {manifest.uploaded_count}/{len(manifest.chunks)} "
                f"chunks already uploaded"
            )
        else:
            if manifest:
                logger.info(
                    "File changed since last upload attempt, starting fresh"
                )
                await self._cleanup_remote_chunks(epoch)
            # Create new manifest
            manifest = await self._create_manifest(
                file_path=file_path,
                epoch=epoch,
                tick=tick,
                file_checksum=file_checksum,
                staging_dir=staging_dir,
            )

        # Step 3: Split file into chunks (if needed)
        if not self._chunks_exist_locally(staging_dir, manifest):
            logger.info(f"Splitting {file_path.name} into chunks...")
            await asyncio.to_thread(
                self._split_file, file_path, staging_dir, manifest
            )
        else:
            logger.info("Chunk files already exist locally")

        # Step 4: Ensure remote directory exists
        await self._ensure_remote_chunks_dir(epoch)

        # Step 5: Save manifest to remote (for resume tracking)
        manifest.status = "uploading"
        await self._save_remote_manifest(epoch, manifest)

        # Step 6: Upload pending chunks
        upload_success = await self._upload_pending_chunks(
            manifest=manifest,
            staging_dir=staging_dir,
            epoch=epoch,
        )
        if not upload_success:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Failed to upload all chunks",
            )

        # Step 7: Reassemble on remote
        logger.info("Reassembling chunks on remote server...")
        manifest.status = "assembling"
        await self._save_remote_manifest(epoch, manifest)

        reassemble_success = await self._reassemble_on_remote(
            manifest=manifest,
            epoch=epoch,
            remote_key=remote_key,
        )
        if not reassemble_success:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Failed to reassemble chunks on remote",
            )

        # Step 8: Verify checksum on remote
        logger.info("Verifying remote file checksum...")
        verify_ok = await self._verify_remote_checksum(
            remote_key=remote_key,
            expected_checksum=file_checksum,
        )
        if not verify_ok:
            manifest.status = "failed"
            await self._save_remote_manifest(epoch, manifest)
            return UploadResult(
                success=False,
                error_message="Remote checksum verification failed",
            )

        # Step 9: Cleanup
        logger.info("Cleaning up chunks...")
        manifest.status = "complete"
        await self._save_remote_manifest(epoch, manifest)
        await self._cleanup_remote_chunks(epoch)
        self._cleanup_local_chunks(staging_dir, manifest)

        logger.info(
            f"Chunked upload complete: {file_size} bytes in "
            f"{len(manifest.chunks)} chunks"
        )

        return UploadResult(
            success=True,
            remote_url=f"{self._target()}:{self._remote_path(remote_key)}",
            bytes_uploaded=file_size,
        )

    def _compute_file_checksum(self, file_path: Path) -> str:
        """Compute SHA-256 checksum of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(BUFFER_SIZE):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _compute_chunk_checksum(self, chunk_path: Path) -> str:
        """Compute SHA-256 checksum of a chunk file."""
        return self._compute_file_checksum(chunk_path)

    async def _create_manifest(
        self,
        file_path: Path,
        epoch: int,
        tick: int,
        file_checksum: str,
        staging_dir: Path,
    ) -> ChunkManifest:
        """Create a new chunk manifest for a file."""
        from datetime import datetime, timezone

        file_size = file_path.stat().st_size
        num_chunks = (file_size + self._chunk_size - 1) // self._chunk_size
        base_name = file_path.name

        chunks = []
        offset = 0
        for i in range(num_chunks):
            chunk_size = min(self._chunk_size, file_size - offset)
            chunk_filename = f"{base_name}.part{i:02d}"
            chunks.append(
                ChunkEntry(
                    index=i,
                    filename=chunk_filename,
                    size=chunk_size,
                    checksum="",  # Will be computed during split
                    uploaded=False,
                )
            )
            offset += chunk_size

        return ChunkManifest(
            epoch=epoch,
            tick=tick,
            archive_name=base_name,
            total_size=file_size,
            chunk_size=self._chunk_size,
            checksum=file_checksum,
            status="chunking",
            chunks=chunks,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    def _chunks_exist_locally(
        self, staging_dir: Path, manifest: ChunkManifest
    ) -> bool:
        """Check if all chunk files exist locally."""
        for chunk in manifest.chunks:
            chunk_path = staging_dir / chunk.filename
            if not chunk_path.exists():
                return False
            if chunk_path.stat().st_size != chunk.size:
                return False
        return True

    def _split_file(
        self,
        file_path: Path,
        staging_dir: Path,
        manifest: ChunkManifest,
    ) -> None:
        """Split a file into chunks and update manifest with checksums."""
        with open(file_path, "rb") as src:
            for chunk in manifest.chunks:
                chunk_path = staging_dir / chunk.filename
                sha256 = hashlib.sha256()

                with open(chunk_path, "wb") as dst:
                    remaining = chunk.size
                    while remaining > 0:
                        buf_size = min(BUFFER_SIZE, remaining)
                        data = src.read(buf_size)
                        if not data:
                            break
                        dst.write(data)
                        sha256.update(data)
                        remaining -= len(data)

                chunk.checksum = sha256.hexdigest()
                logger.debug(
                    f"Created chunk {chunk.filename} "
                    f"({chunk.size} bytes, checksum={chunk.checksum[:8]}...)"
                )

    async def _ensure_remote_chunks_dir(self, epoch: int) -> None:
        """Ensure the remote chunks directory exists."""
        remote_dir = self._remote_path(self._chunks_remote_dir(epoch))
        code, _, stderr = await self._run_ssh(
            "mkdir", "-p", remote_dir, timeout=30
        )
        if code != 0:
            logger.warning(
                f"Failed to create remote chunks dir: {stderr.strip()}"
            )

    async def _load_remote_manifest(
        self, epoch: int
    ) -> Optional[ChunkManifest]:
        """Load manifest from remote server."""
        content = await self.get_small_file(self._manifest_remote_key(epoch))
        if content:
            try:
                return ChunkManifest.from_json(content.decode())
            except Exception as e:
                logger.warning(f"Failed to parse remote manifest: {e}")
        return None

    async def _save_remote_manifest(
        self, epoch: int, manifest: ChunkManifest
    ) -> bool:
        """Save manifest to remote server."""
        content = manifest.to_json().encode()
        return await self.put_small_file(
            self._manifest_remote_key(epoch), content
        )

    async def _upload_pending_chunks(
        self,
        manifest: ChunkManifest,
        staging_dir: Path,
        epoch: int,
    ) -> bool:
        """Upload all pending chunks with optional parallelism."""
        pending = manifest.pending_chunks
        if not pending:
            logger.info("All chunks already uploaded")
            return True

        total = len(manifest.chunks)
        pending_bytes = sum(c.size for c in pending)
        logger.info(
            f"Starting upload: {len(pending)}/{total} chunks "
            f"({pending_bytes / (1024**3):.2f} GB to upload, "
            f"{self._parallel_chunks} parallel)"
        )

        upload_start_time = time.monotonic()
        semaphore = asyncio.Semaphore(self._parallel_chunks)
        failed = False

        async def upload_one(chunk: ChunkEntry) -> bool:
            nonlocal failed
            if failed:
                return False
            async with semaphore:
                success = await self._upload_single_chunk(
                    chunk=chunk,
                    staging_dir=staging_dir,
                    epoch=epoch,
                    manifest=manifest,
                    upload_start_time=upload_start_time,
                )
                if not success:
                    failed = True
                return success

        tasks = [upload_one(c) for c in pending]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check results
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(
                    f"Chunk {pending[i].filename} upload raised: {result}"
                )
                return False
            if not result:
                return False

        # Final summary
        total_duration = time.monotonic() - upload_start_time
        avg_speed = (pending_bytes / total_duration) / (1024 * 1024) if total_duration > 0 else 0
        logger.info(
            f"Upload complete: {pending_bytes / (1024**3):.2f} GB in {total_duration:.0f}s "
            f"(avg {avg_speed:.1f} MB/s)"
        )

        return True

    async def _upload_single_chunk(
        self,
        chunk: ChunkEntry,
        staging_dir: Path,
        epoch: int,
        manifest: ChunkManifest,
        upload_start_time: float,
    ) -> bool:
        """Upload a single chunk with dedicated timeout."""
        chunk_path = staging_dir / chunk.filename
        if not chunk_path.exists():
            logger.error(f"Chunk file not found: {chunk_path}")
            return False

        remote_chunks_dir = self._chunks_remote_dir(epoch)
        remote_chunk_key = f"{remote_chunks_dir}/{chunk.filename}"
        scp_target = f"{self._target()}:{self._remote_path(remote_chunk_key)}"

        cmd = ["scp"] + self._scp_opts() + [str(chunk_path), scp_target]

        chunk_start = time.monotonic()

        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                _, stderr = await asyncio.wait_for(
                    proc.communicate(), timeout=self._chunk_timeout
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                logger.error(
                    f"Chunk {chunk.filename} upload timed out "
                    f"after {self._chunk_timeout}s"
                )
                return False

            if proc.returncode != 0:
                err = stderr.decode().strip()
                logger.error(
                    f"Chunk {chunk.filename} upload failed: "
                    f"exit {proc.returncode}: {err}"
                )
                return False

            # Verify chunk checksum on remote server
            remote_chunk_path = self._remote_path(remote_chunk_key)
            if not await self._verify_remote_chunk_checksum(
                remote_chunk_path, chunk.checksum, chunk.filename
            ):
                logger.error(
                    f"Chunk {chunk.filename} checksum verification failed - "
                    "data may be corrupted during transfer"
                )
                # Delete corrupted chunk on remote
                await self._run_ssh(f"rm -f {remote_chunk_path}", timeout=30)
                return False

            # Mark as uploaded and save manifest
            chunk.uploaded = True
            await self._save_remote_manifest(epoch, manifest)

            # Calculate progress and speed
            chunk_duration = time.monotonic() - chunk_start
            total_elapsed = time.monotonic() - upload_start_time
            speed_mbps = (chunk.size / chunk_duration) / (1024 * 1024) if chunk_duration > 0 else 0
            uploaded_bytes = manifest.uploaded_bytes
            total_bytes = manifest.total_size
            pct = (uploaded_bytes / total_bytes * 100) if total_bytes > 0 else 0

            logger.info(
                f"Uploaded chunk {manifest.uploaded_count}/{len(manifest.chunks)} "
                f"({pct:.0f}%) - {uploaded_bytes / (1024**3):.2f}/{total_bytes / (1024**3):.2f} GB "
                f"@ {speed_mbps:.1f} MB/s - {total_elapsed:.0f}s elapsed"
            )
            return True

        except Exception as e:
            logger.error(f"Chunk {chunk.filename} upload error: {e}")
            return False

    async def _reassemble_on_remote(
        self,
        manifest: ChunkManifest,
        epoch: int,
        remote_key: str,
    ) -> bool:
        """Concatenate chunks into final file on remote server."""
        chunks_dir = self._remote_path(self._chunks_remote_dir(epoch))
        final_path = self._remote_path(remote_key)
        tmp_path = f"{final_path}.tmp"

        # Build cat command with all chunk files in order
        chunk_files = " ".join(
            f"{chunks_dir}/{c.filename}"
            for c in sorted(manifest.chunks, key=lambda x: x.index)
        )

        # Use cat to concatenate, then atomic move
        cat_cmd = f"cat {chunk_files} > {tmp_path} && mv -f {tmp_path} {final_path}"

        logger.debug(f"Reassemble command: {cat_cmd}")

        try:
            code, stdout, stderr = await self._run_ssh(
                cat_cmd, timeout=self._timeout
            )
            if code != 0:
                logger.error(
                    f"Reassembly failed (exit {code}): {stderr.strip()}"
                )
                return False

            # Verify size
            code, stdout, stderr = await self._run_ssh(
                f"stat -c '%s' {final_path}", timeout=30
            )
            if code == 0:
                remote_size = int(stdout.strip())
                if remote_size != manifest.total_size:
                    logger.error(
                        f"Size mismatch: expected {manifest.total_size}, "
                        f"got {remote_size}"
                    )
                    return False
                logger.info(
                    f"Reassembly complete: {remote_size} bytes"
                )
            return True

        except asyncio.TimeoutError:
            logger.error(
                f"Reassembly timed out after {self._timeout}s"
            )
            return False
        except Exception as e:
            logger.error(f"Reassembly error: {e}")
            return False

    async def _verify_remote_chunk_checksum(
        self,
        remote_path: str,
        expected_checksum: str,
        chunk_name: str,
    ) -> bool:
        """Verify the checksum of a single chunk on remote server."""
        try:
            code, stdout, stderr = await self._run_ssh(
                f"sha256sum {remote_path}", timeout=120
            )
            if code != 0:
                logger.error(
                    f"Remote chunk checksum command failed for {chunk_name}: "
                    f"{stderr.strip()}"
                )
                return False

            remote_checksum = stdout.strip().split()[0]
            if remote_checksum != expected_checksum:
                logger.error(
                    f"Chunk {chunk_name} checksum mismatch: "
                    f"expected {expected_checksum[:16]}..., "
                    f"got {remote_checksum[:16]}..."
                )
                return False

            logger.debug(f"Chunk {chunk_name} checksum verified")
            return True

        except asyncio.TimeoutError:
            logger.error(f"Chunk {chunk_name} checksum verification timed out")
            return False
        except Exception as e:
            logger.error(f"Chunk {chunk_name} checksum verification error: {e}")
            return False

    async def _verify_remote_checksum(
        self,
        remote_key: str,
        expected_checksum: str,
    ) -> bool:
        """Verify the checksum of the reassembled file on remote."""
        remote_path = self._remote_path(remote_key)

        try:
            code, stdout, stderr = await self._run_ssh(
                f"sha256sum {remote_path}", timeout=300
            )
            if code != 0:
                logger.error(
                    f"Remote checksum command failed: {stderr.strip()}"
                )
                return False

            remote_checksum = stdout.strip().split()[0]
            if remote_checksum != expected_checksum:
                logger.error(
                    f"Checksum mismatch: expected {expected_checksum[:16]}..., "
                    f"got {remote_checksum[:16]}..."
                )
                return False

            logger.info("Remote checksum verified successfully")
            return True

        except asyncio.TimeoutError:
            logger.error("Remote checksum verification timed out")
            return False
        except Exception as e:
            logger.error(f"Remote checksum verification error: {e}")
            return False

    async def _cleanup_remote_chunks(self, epoch: int) -> bool:
        """Remove the remote chunks directory."""
        chunks_dir = self._chunks_remote_dir(epoch)
        return await self.delete_remote_dir(chunks_dir)

    def _cleanup_local_chunks(
        self,
        staging_dir: Path,
        manifest: ChunkManifest,
    ) -> None:
        """Remove local chunk files."""
        for chunk in manifest.chunks:
            chunk_path = staging_dir / chunk.filename
            try:
                if chunk_path.exists():
                    chunk_path.unlink()
            except OSError as e:
                logger.warning(
                    f"Failed to delete local chunk {chunk.filename}: {e}"
                )

    def get_name(self) -> str:
        return "chunked_scp"
