use std::ffi::OsStr;
use std::time::Duration;
use std::{path::PathBuf, process::ExitCode};

use clap::Parser;
use fractal_fuse::abi::FUSE_ROOT_ID;
use fractal_fuse::{
    DirectoryEntry, DirectoryEntryPlus, ENOENT, FileAttr, FileType, FsResult, MountOptions,
    ReplyAttr, ReplyEntry, ReplyOpen, ReplyStatfs, Request, Timestamp,
};

#[derive(Parser)]
#[command(version)]
struct Args {
    path: PathBuf,
}

fn main() -> ExitCode {
    if let Err(e) = run() {
        eprintln!("{:#}", e);
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> anyhow::Result<()> {
    let args = Args::parse();
    fractal_fuse::Session::new(MountOptions::new().fs_name("jj"))
        .queue_depth(128)
        .run(Fs, &args.path)?;
    Ok(())
}

struct Fs;

impl fractal_fuse::Filesystem for Fs {
    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> FsResult<ReplyEntry> {
        if parent == FUSE_ROOT_ID && name == "hello" {
            Ok(ReplyEntry {
                ttl: TTL,
                attr: hello_attr(),
                generation: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FsResult<ReplyAttr> {
        match inode {
            FUSE_ROOT_ID => Ok(ReplyAttr {
                ttl: TTL,
                attr: root_attr(),
            }),
            HELLO_INO => Ok(ReplyAttr {
                ttl: TTL,
                attr: hello_attr(),
            }),
            _ => Err(ENOENT),
        }
    }

    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        if inode == HELLO_INO {
            Ok(ReplyOpen {
                fh: 0,
                flags: 0,
                backing_id: 0,
            })
        } else {
            Err(ENOENT)
        }
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        buf: &mut [u8],
    ) -> FsResult<usize> {
        if inode != HELLO_INO {
            return Err(ENOENT);
        }
        let offset = offset as usize;
        if offset >= HELLO_CONTENT.len() {
            return Ok(0);
        }
        let end = (offset + buf.len()).min(HELLO_CONTENT.len());
        let src = &HELLO_CONTENT[offset..end];
        buf[..src.len()].copy_from_slice(src);
        Ok(src.len())
    }

    async fn readdir(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntry>> {
        if inode != FUSE_ROOT_ID {
            return Err(ENOENT);
        }
        let entries = vec![
            DirectoryEntry {
                ino: FUSE_ROOT_ID,
                offset: 1,
                kind: FileType::Directory,
                name: b".".to_vec(),
            },
            DirectoryEntry {
                ino: FUSE_ROOT_ID,
                offset: 2,
                kind: FileType::Directory,
                name: b"..".to_vec(),
            },
            DirectoryEntry {
                ino: HELLO_INO,
                offset: 3,
                kind: FileType::RegularFile,
                name: b"hello".to_vec(),
            },
        ];
        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    async fn readdirplus(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        _size: u32,
    ) -> FsResult<Vec<DirectoryEntryPlus>> {
        if inode != FUSE_ROOT_ID {
            return Err(ENOENT);
        }
        let entries = vec![
            DirectoryEntryPlus {
                ino: FUSE_ROOT_ID,
                offset: 1,
                kind: FileType::Directory,
                name: b".".to_vec(),
                entry_ttl: TTL,
                attr: root_attr(),
                generation: 0,
            },
            DirectoryEntryPlus {
                ino: FUSE_ROOT_ID,
                offset: 2,
                kind: FileType::Directory,
                name: b"..".to_vec(),
                entry_ttl: TTL,
                attr: root_attr(),
                generation: 0,
            },
            DirectoryEntryPlus {
                ino: HELLO_INO,
                offset: 3,
                kind: FileType::RegularFile,
                name: b"hello".to_vec(),
                entry_ttl: TTL,
                attr: hello_attr(),
                generation: 0,
            },
        ];
        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    async fn opendir(&self, _req: Request, _inode: u64, _flags: u32) -> FsResult<ReplyOpen> {
        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
            backing_id: 0,
        })
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FsResult<ReplyStatfs> {
        Ok(ReplyStatfs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 2,
            ffree: 0,
            bsize: 512,
            namelen: 255,
            frsize: 512,
        })
    }

    async fn access(&self, _req: Request, _inode: u64, _mask: u32) -> FsResult<()> {
        Ok(())
    }
}

fn root_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: FUSE_ROOT_ID,
        size: 0,
        blocks: 0,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::Directory.to_mode() | 0o755,
        nlink: 2,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 512,
    }
}

fn hello_attr() -> FileAttr {
    let ts = now_ts();
    FileAttr {
        ino: HELLO_INO,
        size: HELLO_CONTENT.len() as u64,
        blocks: 1,
        atime: ts,
        mtime: ts,
        ctime: ts,
        mode: FileType::RegularFile.to_mode() | 0o444,
        nlink: 1,
        uid: unsafe { libc::getuid() },
        gid: unsafe { libc::getgid() },
        rdev: 0,
        blksize: 512,
    }
}

fn now_ts() -> Timestamp {
    let d = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp::new(d.as_secs(), d.subsec_nanos())
}

const TTL: Duration = Duration::from_secs(60);
const HELLO_INO: u64 = 2;
const HELLO_CONTENT: &[u8] = b"Hello, FUSE over io_uring!\n";
